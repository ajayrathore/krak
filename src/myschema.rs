use serde::{Serialize, Deserialize};

#[derive(Default, Debug, Deserialize, Serialize)]
pub struct Trade {
    pub sid: i32,
    pub sym: String,
    pub price: f64,
    pub size: i64
}


#[derive(Debug, Deserialize, Serialize)]
pub struct Test {
    pub a: i64,
    pub b: String,
}