use crate::schema::{FieldType, Schema, TIME_COL_NAME};
use anyhow::Result;
use arrow::array::StringArray;
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct SchemaBuilder {
    domain: Option<String>,
    fields: Vec<ArrowField>,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn domain(mut self, domain: impl Into<String>) -> Self {
        self.domain = Some(domain.into());
        self
    }

    pub fn field(mut self, name: &str, field_type: impl Into<ArrowDataType>) -> Self {
        self.add_column(name, field_type.into(), false)
    }

    pub fn timestamp(mut self) -> Self {
        self.add_column(TIME_COL_NAME, FieldType::Time.into(), false)
    }

    pub fn build(self) -> Result<Schema> {
        Schema::new_with_fields(self.domain, self.fields)
    }

    fn add_column(mut self, name: &str, field_type: ArrowDataType, nullable: bool) -> Self {
        self.fields
            .push(ArrowField::new(name, field_type, nullable));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_builder() {
        let schema = SchemaBuilder::new()
            .field("int_field", FieldType::Int)
            .field("str_field", FieldType::Str)
            .field("unknown_field", ArrowDataType::Int64)
            .timestamp()
            .domain("apache")
            .build();

        assert_eq!(
            true,
            schema.is_err(),
            "should fail, as Int64 is not supported. error: {:?}",
            schema.err().unwrap().to_string()
        );
    }

    #[test]
    fn test_builder_with_record_barch() {
        let schema = SchemaBuilder::new()
            .field("int_field", FieldType::Int)
            .field("str_field", FieldType::Str)
            //   .field("unknown_field", ArrowDataType::Int64)
            .timestamp()
            .domain("apache")
            .build()
            .unwrap();

        let array = StringArray::from(vec!["hello", "world", "bye"]);

        let batch = RecordBatch::try_new(Arc::new(schema.inner()), vec![Arc::new(array)]);

        assert_eq!(
            true,
            batch.is_ok(),
            "should fail, as Int64 is not supported. error: {:?}",
            batch.err().unwrap().to_string()
        );
    }
}
