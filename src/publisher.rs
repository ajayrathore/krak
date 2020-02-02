use crate::encoder::*;
use crate::util::*;
use rkdb::{kbindings::*, types::*};
use kafka::producer::{Producer, Record, RequiredAcks};
use std::time::Duration;
use lazy_static::lazy_static;
use std::sync::Mutex;
use schema_registry_converter::schema_registry;


lazy_static! {
    static ref PRODUCER : Mutex<Producer> = Mutex::new(Producer::from_hosts(vec!(get_kafka_broker().to_owned()))
        .with_ack_timeout(Duration::from_secs(1))
        .with_required_acks(RequiredAcks::One)
        .create()
        .unwrap());
}


#[no_mangle]
pub extern "C" fn publish(tbl: *const K, rows: *const K, colnames: *const K) -> *const K {
    let payload = encode_table(tbl, rows, colnames);
    let mut records = Vec::new();
    for record in payload{
        records.push(Record::from_value("trades", record));
    }
    let result = PRODUCER.lock().unwrap().send_all(&records).unwrap();
    println!("publish status = {:?}", result);
    kvoid()
}


#[no_mangle]
pub extern "C" fn post_schema(sregurl: *const K, raw_schema: *const K) -> *const K {
    if let KVal::String(url) = KVal::new(sregurl) {
        if let KVal::String(schema) = KVal::new(raw_schema) {
            let schema = schema_registry::SuppliedSchema::new(String::from(schema));
            let furl = get_schema_registry() + "/" + url;
            println!("schema posting to {}", furl);
            let result = schema_registry::post_schema(furl.as_ref(), schema);
            println!("Schema posted, Received Id: {:?}", result);
        }
    }
    kvoid()
}