use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use rkdb::kbindings::{KVal, KData, kvoid, kdict};
use rkdb::types::{K};
use schema_registry_converter::Decoder;
use std::{thread, ffi};
use avro_rs::types::Value;
use rkdb::k::k;

#[no_mangle]
pub extern "C" fn receiver_init(callback: *const K) -> *const K {
    let mut cbk_func = String::new();
    match KVal::new(callback) {
        KVal::String(cbk) => cbk_func.push_str(cbk),
        _ => {println!("Invalid callback, pass function name as string"); return kvoid()}
    }

    let mut consumer : Consumer =
        Consumer::from_hosts(vec!("localhost:9092".to_owned()))
            .with_topic_partitions("trades".to_owned(), &[0])
            .with_fallback_offset(FetchOffset::Earliest)
            .with_offset_storage(GroupOffsetStorage::Kafka)
            .with_fetch_max_bytes_per_partition(1000012)
            .create()
            .unwrap();

    let mut decoder = Decoder::new("localhost:8081".to_string());

    thread::spawn(move || {
        loop {
            for ms in consumer.poll().unwrap().iter() {
                for m in ms.messages() {
                    let payload = decoder.decode(Some(&m.value)).unwrap();
                    println!("Decoded : {:?}", payload);
                    match payload {
                        Value::Record(mut v) => {
                            let mut keys : Vec<String> = Vec::new();
                            let mut values : Vec<KVal> = Vec::new();
                            for (k, v) in v.iter_mut(){
                                keys.push(k.parse().unwrap());
                                match v {
                                    Value::Int(i) => { values.push(KVal::Int(KData::Atom(i)))},
                                    Value::Long(l) => {values.push(KVal::Long(KData::Atom(l)))},
                                    Value::Double(d) => {values.push(KVal::Float(KData::Atom(d)))},
                                    Value::String(s) => values.push(KVal::String(&s[0..])),
                                    _ => println!("Unrecognized type received")
                                }
                            }
                            let kkeys = KVal::Symbol(KData::List(&mut keys));
                            let kvals = KVal::Mixed(values);
                            let kret = kdict(&kkeys, &kvals);
                            unsafe { k(0, ffi::CString::new(cbk_func.as_bytes().to_vec()).unwrap().as_ptr(), kret, 0); }
                        }
                        _ => println!("Did not receive a record")
                    }
                }
                consumer.consume_messageset(ms);
            }
            consumer.commit_consumed().unwrap();
        }
    });
    kvoid()
}