use crate::schema::*;
use rkdb::{kbindings::*, types::*};
use avro_rs::{Codec, Schema, Writer};
use schema_registry_converter::schema_registry::SubjectNameStrategy;
use schema_registry_converter::Encoder;
use avro_rs::types::Value;


#[no_mangle]
pub extern "C" fn encode_table(tbl: *const K, rows: *const K, colnames: *const K) -> Vec<Vec<u8>> {
    let mut result : Vec<Vec<u8>> = Vec::new();
    let mut nr = 0;
    let mut cnames:Vec<String> = Vec::new();
    match KVal::new(rows) {
        KVal::Int(KData::Atom(r)) => nr=*r,
        _ => println!("Invalid rows")
    };
    if let KVal::Mixed(v) = KVal::new(colnames) {
        for i in v.iter(){
            if let KVal::String(s) = i {
                cnames.push(s.parse().unwrap());
            }
        }
    }
    match KVal::new(tbl) {
        KVal::Table(box KVal::Dict(box KVal::Symbol(KData::List(k)), box KVal::Mixed(cols))) => {
            let mut records : Vec<Vec<(&'static str, Value)>> = Vec::new();
            for i in 0..nr{
                let mut record : Vec<(&'static str, Value)> = Vec::new();
                for (index, col) in cols.iter().enumerate(){
                    let key = Box::leak(cnames[index].clone().into_boxed_str());
                    match col {
                        KVal::Int(KData::List(ic)) => record.push((key, Value::Int((ic)[i as usize]))),
                        KVal::Mixed(symbols) => {
                            match symbols[i as usize] {
                                KVal::String(syf) =>  record.push((key, Value::String(syf.parse().unwrap()))),
                                _ => println!("No string symbol")
                            }
                        },
                        KVal::Float(KData::List(pc)) => record.push((key, Value::Double((pc)[i as usize]))),
                        KVal::Long(KData::List(sc)) => record.push((key, Value::Long((sc)[i as usize]))),
                        _ => println!("Unrecognized Col")
                    }
                }
                records.push(record);
            }
            result = encode_trades_with_schema_registry(&records);
        },
        _ => println!("No Table")
    };
    result
}


pub(crate) fn encode_trades_with_schema_registry(records: &Vec<Vec<(&'static str, Value)>>) -> Vec<Vec<u8>>  {
    let value_strategy = SubjectNameStrategy::TopicNameStrategy("trade".into(), false);
    let mut encoder = Encoder::new("localhost:8081".to_string());
    let mut bytes : Vec<Vec<u8>> = Vec::new();
    for record in records.iter(){
        let encoded = encoder.encode(record.to_vec(), &value_strategy);
        bytes.push(encoded.unwrap());
    }
    bytes
}


#[cfg(test)]
mod tests {
    use super::*;
    use failure::_core::fmt::Error;
    use avro_rs::types::Record;
    use avro_rs::{Reader, from_value};
    use schema_registry_converter::Decoder;

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
            println!("{:?}", from_value::<Test>(&record.unwrap()).unwrap());
        }
        Ok(())
    }

    #[test]
    fn test_encodedecode_trades() -> Result<(), Error> {
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
        let mut decoder = Decoder::new("localhost:8081".to_string());
        for bytes in payload{
            let res = decoder.decode(Some(&bytes)).unwrap();
            println!("Decoded : {:?}", res);
        }

        Ok(())
    }
}
