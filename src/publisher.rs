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
pub extern "C" fn publish(tbl: *const K, rows: *const K) -> *const K {
    let payload = encode_table(tbl, rows);
    let mut records = Vec::new();
    for record in payload{
        records.push(Record::from_value("trades", record));
    }
    let result = PRODUCER.lock().unwrap().send_all(&records).unwrap();
    println!("publish status = {:?}", result);
    kvoid()
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::Error;
    use crate::schema::Trade;

    #[test]
    fn test_publish_trades() -> Result<(), Error> {
        let trade1 = Trade {
            sid: 1,
            sym: String::from("msft"),
            price: 22.4f64,
            size: 786i64
        };

        let trade2 = Trade {
            sid: 2,
            sym: String::from("hsbc"),
            price: 99.4f64,
            size: 654i64
        };

        let trades = vec![trade1, trade2];
        let payload = encode_trades_with_schema_registry(&trades);
        println!("payload : {:?}", payload);
        let mut producer = Producer::from_hosts(vec!("localhost:9092".to_owned()))
            .with_ack_timeout(Duration::from_secs(1))
            .with_required_acks(RequiredAcks::One)
            .create()
            .unwrap();
        let mut records = Vec::new();
        for record in payload{
            records.push(Record::from_value("trades", record));
        }
        let result = producer.send_all(&records).unwrap();
        print!("status={:?}", result);

        Ok(())
    }
}