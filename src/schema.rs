use serde::{Serialize, Deserialize};


pub static RAW_SCHEMA: &str = r#"
            {
                "type": "record",
                "name": "trade",
                "fields": [
                    {"name": "sid", "type": "int"},
                    {"name": "sym", "type": "string"},
                    {"name": "price", "type": "double"},
                    {"name": "size", "type": "long"}
                ]
            }
        "#;


#[derive(Default, Debug, Deserialize, Serialize)]
pub struct Trade {
    pub sid: i32,
    pub sym: String,
    pub price: f64,
    pub size: i64
}


#[derive(Debug, Deserialize, Serialize)]
pub struct Test {
    pub a: i64,
    pub b: String,
}