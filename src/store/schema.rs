struct Schema {
    columns: HashMap<String, ColumnInfo>,
}

impl Schema {
    fn get_or_insert_col(&mut self, col_name: String, _type: ColumnType) -> &ColumnInfo {
        let _id = self.columns.keys().len(); //use current size of map as id
        let column_info = self
            .columns
            .entry(col_name.clone())
            .or_insert(ColumnInfo { _type, _id });
        column_info
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Table<'a> {
    name: &'a str,
    schema: Schema, //map of key v id
}
