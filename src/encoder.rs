use crate::myschema::*;
use rkdb::{kbindings::*, types::*};
use avro_rs::{Codec, Schema, Writer};


#[no_mangle]
pub extern "C" fn encode_table(tbl: *const K, rows: *const K) -> *const K {
    let mut result = kvoid();
    let mut nr = 0;
    match KVal::new(rows) {
        KVal::Int(KData::Atom(r)) => nr=*r,
        _ => println!("Invalid rows")
    };
    match KVal::new(tbl) {
        KVal::Table(box b) => {
            match b {
                KVal::Dict(_, box v) => {
                    match v {
                        KVal::Mixed(cols) => {
                            let mut records : Vec<Trade> = Vec::new();
                            for i in 0..nr{
                                let mut record : Trade = Default::default();
                                for col in cols.iter(){
                                    match col {
                                        KVal::Int(KData::List(ic)) => record.sid = (ic)[i as usize],
                                        KVal::Mixed(symbols) => {
                                            match symbols[i as usize] {
                                                KVal::String(syf) => record.sym = syf.parse().unwrap(),
                                                _ => println!("No string symbol")
                                            }
                                        },
                                        KVal::Float(KData::List(pc)) => record.price = pc[i as usize],
                                        KVal::Long(KData::List(sc)) => record.size = sc[i as usize],
                                        _ => println!("Unrecognized Col")
                                    }
                                }
                                //println!("record = {:?}", record);
                                records.push(record);
                            }
                            let encoded = encode_trades(&records);
                            result = klist(4, &encoded[..])
                        },
                        _ => println!("No cols found")
                    }
                },
                _ => println!("no dict ")
            }
        },
        _ => println!("No match")
    };
    result
}


fn encode_trades(trades: &Vec<Trade>) -> Vec<u8>  {
    let raw_schema = r#"
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
    let schema = Schema::parse_str(raw_schema).unwrap();
    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

    for trade in trades.into_iter(){
        writer.append_ser(trade).unwrap();
    }
    writer.flush().unwrap();
    let encoded = writer.into_inner();
    encoded
}


#[cfg(test)]
mod tests {
    use super::*;
    use failure::_core::fmt::Error;
    use avro_rs::types::Record;

    #[test]
    fn test_avro_rw() -> Result<(), Error> {
        let raw_schema = r#"
            {
                "type": "record",
                "name": "test",
                "fields": [
                    {"name": "a", "type": "long", "default": 42},
                    {"name": "b", "type": "string"}
                ]
            }
        "#;

        let schema = Schema::parse_str(raw_schema).unwrap();

        println!("{:?}", schema);

        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Deflate);

        let mut record = Record::new(writer.schema()).unwrap();
        record.put("a", 27i64);
        record.put("b", "foo");

        writer.append(record).unwrap();

        let test = Test {
            a: 27,
            b: "foo".to_owned(),
        };

        writer.append_ser(test).unwrap();

        writer.flush().unwrap();

        let input = writer.into_inner();
        println!("input={:?}", input);
        let reader = Reader::with_schema(&schema, &input[..]).unwrap();

        for record in reader {
            println!("{:?}", from_value::<Test>(&record?));
        }
        Ok(())
    }
}
