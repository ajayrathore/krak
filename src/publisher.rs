use crate::encoder::*;
use crate::util::*;
use rkdb::{kbindings::*, types::*};
use kafka::producer::{Producer, Record, RequiredAcks};
use std::time::Duration;
use lazy_static::lazy_static;
use std::sync::Mutex;
use schema_registry_converter::schema_registry::{SuppliedSchema, post_schema};


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


// from q
//postschema["localhost:8081/subjects/quote-value/versions";.j.j `type`name`fields!(`record;`quote;flip(`name`type!(`sid`sym`bid`ask;`int`string`double`double)))]
#[no_mangle]
pub extern "C" fn postschema(sregurl: *const K, raw_schema: *const K) -> *const K {
    if let KVal::String(url) = KVal::new(sregurl) {
        if let KVal::String(schema) = KVal::new(raw_schema) {
            let schema = SuppliedSchema::new(String::from(schema));
            let result = post_schema(url, schema);
            println!("Schema posted, Received Id: {:?}", result);
        }
    }
    kvoid()
}