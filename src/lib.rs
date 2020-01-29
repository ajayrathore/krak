#![feature(box_patterns)]
#![feature(box_syntax)]

extern crate avro_rs;
extern crate rkdb;

pub mod schema;
pub mod encoder;
pub mod publisher;
pub mod receiver;


mod tests {
    use super::*;
    use failure::_core::fmt::Error;
    use avro_rs::types::Record;
    use avro_rs::{Reader, from_value};
    use schema_registry_converter::schema_registry::{SuppliedSchema, post_schema};
    use crate::schema::RAW_SCHEMA;

    #[test]
    fn test_post_schema_to_registry() -> Result<(), Error> {
        let trade_schema = SuppliedSchema::new(String::from(RAW_SCHEMA));
        let result = post_schema("localhost:8081/subjects/trade-value/versions", trade_schema);
        println!("Received Id: {:?}", result);
        Ok(())
    }
}