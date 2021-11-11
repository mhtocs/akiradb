//! MOSTLY BASED ON SCHEMA from INFLUX IOX

use anyhow::anyhow;
use anyhow::Result;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use std::convert::{TryFrom, TryInto};

pub const TIME_COL_NAME: &str = "time";

#[derive(Debug)]
pub struct Schema {
    inner: ArrowSchema,
}

impl Schema {
    pub fn new(inner: ArrowSchema) -> Self {
        Self { inner }
    }

    pub fn iter(&self) -> SchemaIterator<'_> {
        SchemaIterator::new(self)
    }

    pub fn is_supported_type(arrow_data_type: &ArrowDataType) -> bool {
        TryInto::<FieldType>::try_into(arrow_data_type).is_ok()
    }

    pub fn new_with_fields(domain: Option<String>, fields: Vec<ArrowField>) -> Result<Self> {
        Self::try_from(ArrowSchema::new(fields))
    }

    pub fn inner(self) -> ArrowSchema {
        self.inner
    }
}

impl TryFrom<ArrowSchema> for Schema {
    type Error = anyhow::Error;
    fn try_from(inner: ArrowSchema) -> Result<Self, Self::Error> {
        // add check for duplicate field
        for field in inner.fields() {
            if !Schema::is_supported_type(field.data_type()) {
                let error = format!(
                    "Unsupported data type({}) used for column: {}",
                    field.data_type(),
                    field.name()
                );
                return Err(anyhow!(error));
            }
        }

        Ok(Self { inner })
    }
}

impl From<Schema> for ArrowSchema {
    fn from(schema: Schema) -> Self {
        schema.inner
    }
}

impl From<FieldType> for ArrowDataType {
    fn from(_type: FieldType) -> Self {
        match _type {
            FieldType::Int => ArrowDataType::Int32,
            FieldType::Str => ArrowDataType::Utf8,
            FieldType::Time => ArrowDataType::Int32,
        }
    }
}

#[derive(Debug)]
pub enum FieldType {
    Int,
    Str,
    Time,
}

// we also need way to get field type from string
impl TryFrom<&str> for FieldType {
    type Error = anyhow::Error;
    fn try_from(_type: &str) -> Result<Self, Self::Error> {
        match _type {
            "string" => Ok(FieldType::Str),
            "int" => Ok(FieldType::Int),
            "time" => Ok(FieldType::Time),
            _ => Err(anyhow!("Unknown type")),
        }
    }
}

impl TryFrom<&ArrowDataType> for FieldType {
    type Error = anyhow::Error;
    fn try_from(_type: &ArrowDataType) -> Result<Self, Self::Error> {
        match _type {
            ArrowDataType::Utf8 => Ok(FieldType::Str),
            ArrowDataType::Int32 => Ok(FieldType::Int),
            _ => Err(anyhow!("Unknown type")),
        }
    }
}

#[derive(Debug)]
pub struct SchemaIterator<'a> {
    schema: &'a Schema,
    index: usize,
}

impl<'a> Iterator for SchemaIterator<'a> {
    type Item = &'a ArrowField;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.schema.inner.fields().len() {
            let field = self.schema.inner.field(self.index);
            self.index += 1;
            Some(field)
        } else {
            None
        }
    }
}

impl<'a> SchemaIterator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self { schema, index: 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn try_new_from_arrow() {
        let fields = vec![
            ArrowField::new("INT_COL", ArrowDataType::Int32, false),
            ArrowField::new("STR_COL", ArrowDataType::Utf8, false),
            ArrowField::new("TIME_COL", ArrowDataType::Int32, false),
        ];

        let schema = Schema::try_from(ArrowSchema::new(fields));
        assert_eq!(
            true,
            schema.is_ok(),
            "should be able to create schema from arrow. error: {:?}",
            schema.err().unwrap().to_string()
        );
    }

    #[test]
    fn try_new_from_arrow_with_unsupported_datatype() {
        let fields = vec![
            ArrowField::new("INT_COL", ArrowDataType::Int32, false),
            ArrowField::new("STR_COL", ArrowDataType::Utf8, false),
            ArrowField::new("UNK_COL", ArrowDataType::Int8, false),
        ];

        let schema = Schema::try_from(ArrowSchema::new(fields));
        assert_eq!(
            "Unsupported data type(Int8) used for column: UNK_COL",
            schema.unwrap_err().to_string(),
        );
    }
}
