use std::env;


pub fn get_kafka_broker() -> String {
    let kafka_host = env::var("KAFKA_BROKER_HOST").expect("kafka host not defined");
    let kafka_port = env::var("KAFKA_BROKER_PORT").expect("kafka port not defined");
    let conn = kafka_host + ":" + &kafka_port;
    println!("Using Kafka Broker : {}", conn);
    conn
}


pub fn get_schema_registry() -> String {
    let schema_reg_host = env::var("SCHEMA_REG_HOST").expect("schema reg host not defined");
    let schema_reg_port = env::var("SCHEMA_REG_PORT").expect("schema reg port not defined");
    let conn = "http://".to_owned() + &schema_reg_host.to_owned() + ":" + &schema_reg_port;
    println!("Using Schema Registry : {}", conn);
    conn
}