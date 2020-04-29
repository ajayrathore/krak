use crate::util::*;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use rkdb::kbindings::{KVal, KData, kvoid, kdict, kstring};
use rkdb::types::{K};
use schema_registry_converter::Decoder;
use std::{thread, ffi};
use avro_rs::types::Value;
use rkdb::k::k;
use lazy_static::lazy_static;
use std::sync::Mutex;

lazy_static! {
    static ref DECODER : Mutex<Decoder> = Mutex::new(Decoder::new(get_schema_registry().to_string()));
}


#[no_mangle]
pub extern "C" fn receiver_init(enumasint: *const K, callback: *const K, topic: *const K, partitions: *const K) -> *const K {
    let mut easint  = false;
    if let KVal::Bool(KData::Atom(b)) = KVal::new(enumasint) {
        easint = b.to_owned();
    }

    let mut cbk_func = String::new();
    match KVal::new(callback) {
        KVal::String(cbk) => cbk_func.push_str(cbk),
        _ => {println!("Invalid callback, pass function name as string"); return kvoid()}
    }

    let mut tpc = String::new();
    if let KVal::String(t) = KVal::new(topic) {
        println!("topic received: {}", t);
        tpc.push_str(t);
    }

    let mut parts: &mut [i32] = &mut [0];
    if let KVal::Int(KData::List(p)) = KVal::new(partitions) {
        println!("partitions received: {:?}", p);
        parts = p;
    }

    let mut consumer : Consumer =
        Consumer::from_hosts(vec!(get_kafka_broker().to_owned()))
            .with_topic_partitions(tpc.to_owned(), parts)
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .with_fetch_max_bytes_per_partition(1000012)
            .create()
            .unwrap();

    thread::spawn(move || {
        loop {
            for ms in consumer.poll().unwrap().iter() {
                for m in ms.messages() {
                    let key =  std::str::from_utf8(m.key).unwrap();
                    let kret = parse_msg(&m.value, easint);
                    unsafe { k(0, ffi::CString::new(cbk_func.as_bytes().to_vec()).unwrap().as_ptr(), kstring(key), kret, 0); }
                }
                consumer.consume_messageset(ms).unwrap();
            }
            consumer.commit_consumed().unwrap();
        }
    });
    kvoid()
}


#[no_mangle]
pub extern "C" fn decode(enumasint: *const K, msg: *const K) -> *const K {
    let mut result = kvoid();
    let mut easint  = false;
    if let KVal::Bool(KData::Atom(b)) = KVal::new(enumasint) {
        easint = b.to_owned();
    }
    if let KVal::Byte(KData::List(m)) = KVal::new(msg) {
        result = parse_msg(m, easint);
    }else {println!("MSG not a byte array")}
    result
}


fn parse_msg(data: &[u8], enumasint: bool) -> *const K {
    let mut result = kvoid();
    let payload = DECODER.lock().unwrap().decode(Some(data)).unwrap();
    match payload {
        Value::Record(mut v) => {
            let mut keys: Vec<String> = Vec::new();
            let mut values: Vec<KVal> = Vec::new();
            for (k, v) in v.iter_mut() {
                println!("Key    = {:?}", k);
                println!("Value  = {:?}", v);
                keys.push(k.parse().unwrap());
                match v {
                    Value::Array(arr) => for a in arr.into_iter() {
                      values.push(parse_msgtype(a, enumasint));
                    },
                    _ => values.push(parse_msgtype(v, enumasint))
                }
            }
            let kkeys = KVal::Symbol(KData::List(&mut keys));
            let kvals = KVal::Mixed(values);
            let kret = kdict(&kkeys, &kvals);
            result = kret;
        }
        _ => println!("Did not receive a record")
    }
    result
}


fn parse_msgtype(val: &mut Value, enumasint: bool) -> KVal {
    match val {
        Value::Int(i) => KVal::Int(KData::Atom(i)),
        Value::Long(l) => KVal::Long(KData::Atom(l)),
        Value::Float(f) => KVal::Real(KData::Atom(f)),
        Value::Double(d) => KVal::Float(KData::Atom(d)),
        Value::Boolean(b) => KVal::Bool(KData::Atom(b)),
        Value::String(s) => KVal::String(&s[0..]),
        Value::Null => KVal::String("null"),
        Value::Enum(i, s) =>
            if enumasint
                { KVal::Int(KData::Atom(i))}
            else
                {KVal::Symbol(KData::Atom(s))},
        Value::Union(box u) => parse_msgtype(u, enumasint),
        Value::Record(records) => {
            let mut keys: Vec<KVal> = Vec::new();
            let mut values: Vec<KVal> = Vec::new();
            for (key, val) in records.into_iter() {
                keys.push(KVal::Symbol(KData::Atom(key)));
                values.push(parse_msgtype(val, enumasint));
            }
            KVal::Dict(Box::new(KVal::Mixed(keys)), Box::new(KVal::Mixed(values)))
        },
        Value::Map(map) => {
            let mut keys: Vec<KVal> = Vec::new();
            let mut values: Vec<KVal> = Vec::new();
            for (key, val) in map.into_iter() {
                keys.push(KVal::String(key));
                values.push(parse_msgtype(val, enumasint));
            }
            KVal::Dict(Box::new(KVal::Mixed(keys)), Box::new(KVal::Mixed(values)))
        },
        _ => {println!("Unrecognized msg field received"); KVal::Unknown}
    }
}