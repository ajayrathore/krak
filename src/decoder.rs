use crate::myschema::*;
use rkdb::{kbindings::*, types::*};
use avro_rs::{Reader, Schema, from_value};

#[no_mangle]
pub extern "C" fn decode_table(k: *const K) -> *const K {
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
    let mut result = kvoid();

    match KVal::new(k) {
        KVal::Byte(KData::List(k)) => {
            let reader = Reader::with_schema(&schema, &k[..]).unwrap();
            let mut sids: Vec<i32> = Vec::new();
            let mut syms: Vec<String> = Vec::new();
            let mut prices: Vec<f64> = Vec::new();
            let mut sizes: Vec<i64> = Vec::new();
            for value in reader {
                let t = from_value::<Trade>(&value.unwrap()).unwrap();
                sids.push(t.sid.clone());
                prices.push(t.price.clone());
                sizes.push(t.size.clone());
                syms.push(t.sym);
            };
            let mut col_names  = [String::from("sid"), String::from("sym"), String::from("price"), String::from("size")];
            let sidcol = KVal::Int(KData::List(&mut sids));
            let symcol = KVal::Mixed(syms.iter().map(|s| {KVal::String(s)}).collect());
            let pricecol = KVal::Float(KData::List(&mut prices));
            let sizecol = KVal::Long(KData::List(&mut sizes));

            let k = KVal::Symbol(KData::List(&mut col_names));
            let v = KVal::Mixed(vec!(sidcol, symcol, pricecol, sizecol));
            let d = KVal::Dict(Box::new(k), Box::new(v));
            result = ktable(d);
        },
        _ => println!("cant decode")
    };
    result
}