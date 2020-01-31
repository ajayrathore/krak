use crate::encoder::*;
use rkdb::{kbindings::*, types::*};
use kafka::producer::{Producer, Record, RequiredAcks};
use std::time::Duration;
use lazy_static::lazy_static;
use std::sync::Mutex;

lazy_static! {
    static ref PRODUCER : Mutex<Producer> = Mutex::new(Producer::from_hosts(vec!("localhost:9092".to_owned()))
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