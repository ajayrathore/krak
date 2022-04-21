#![feature(box_patterns)]
#![feature(box_syntax)]

extern crate avro_rs;
extern crate rkdb;
extern crate rdkafka;

pub mod encoder;
pub mod publisher;
pub mod receiver;
pub mod util;