use crate::encoder::*;
use crate::util::*;
use rkdb::{kbindings::*, types::*};
use std::time::Duration;
use lazy_static::lazy_static;
use std::sync::Mutex;
use schema_registry_converter::schema_registry;
use failure::_core::cell::RefCell;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{Producer, BaseProducer, BaseRecord};


lazy_static! {
    static ref PRODUCER : Mutex<BaseProducer> = Mutex::new(ClientConfig::new()
                                                            .set("metadata.broker.list", "localhost")
                                                            .set("security.protocol", "sasl_plaintext")
                                                            .set("sasl.kerberos.kinit.cmd", ":")
                                                            .set("sasl.mechanism", "GSSAPI")
                                                            .set("queue.buffering.max.kbytes", "1000000")
                                                            .set("acks", "1")
                                                            .create()
                                                            .expect("Producer creation error")
                                                          );
}


#[no_mangle]
pub extern "C" fn publish(topic: *const K, msg_key: *const K, tbl: *const K, rows: *const K, colnames: *const K) -> *const K {
    let payload = encode_table(topic, tbl, rows, colnames);

    let raw_key : RefCell<Vec<u8>> = RefCell::new(Vec::new());
    if let KVal::String(key) = KVal::new(msg_key) {
        raw_key.borrow_mut().clone_from(&key.as_bytes().to_vec());
    }

    let mut tpc = String::new();
    if let KVal::String(t) = KVal::new(topic) {
        tpc.push_str(t);
    }

    for record in payload{
        println!("publishing payload ", record.);
        PRODUCER.lock().unwrap().send(BaseRecord::to(&tpc)
                                                  .key(&raw_key.clone().into_inner())
                                                  .payload(&record)
                                     ).expect("Failed to enqueue");
    }
    PRODUCER.lock().unwrap().flush(Duration::from_secs(1));
    println!("flushed");
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