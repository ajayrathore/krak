#![feature(box_patterns)]
#![feature(box_syntax)]

extern crate avro_rs;
extern crate rkdb;

pub mod encoder;
pub mod publisher;
pub mod receiver;


mod tests {
    use schema_registry_converter::schema_registry::{SuppliedSchema, post_schema};
    use failure::_core::fmt::Error;

    #[test]
    fn test_post_schema_to_registry() -> Result<(), Error> {
        let RAW_SCHEMA: &str = r#"
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
        let trade_schema = SuppliedSchema::new(String::from(RAW_SCHEMA));
        let result = post_schema("localhost:8081/subjects/trade-value/versions", trade_schema);
        println!("Received Id: {:?}", result);
        Ok(())
    }
}