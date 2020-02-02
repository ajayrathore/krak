# krak

Avro, kafka, schema_registry binding for kdb+ using Rust.
This rust lib is intended to be loaded inside running q process and used for publishing and consuming avro encoded messages from a kafka broker, it also relies on confluence schema regitry for parsing the messages, more information can be found here https://docs.confluent.io/current/schema-registry/index.html

This library utilies the existing kdb rust binding https://github.com/redsift/rkdb and various other rust libraries for integrating with avro, and kafka

## Building On Mac

```
~/krak(master ✗) cargo build --release && cp target/release/libkrak.dylib ${QHOME}/m64/libkrak.so
```

# Usage from q
Set up following envs

* export KAFKA_BROKER_HOST=localhost
* export KAFKA_BROKER_PORT=9092
* export SCHEMA_REG_PORT=8081


Publishing to kafka broker

```
q)pub : `libkrak 2:(`publish;3)
q)
q)t:([]id:10?100i;sym:string 10?`2;price:10?100f;size:10?100)  // symbols are not supported, pass as strings
q)3#t
id  sym  price    size
----------------------
77  "ci" 8.388858 12
30  "hk" 19.59907 10
17  "ae" 37.5638  1
q)
q)pub[t;`int$count t;string cols t]
Using Kafka Broker : localhost:9092
publish status = [ProduceConfirm { topic: "trades", partition_confirms: [ProducePartitionConfirm { offset: Ok(3212), partition: 0 }] }]

```

Consuming From kafka broker
```
q)receive : `libkrak 2:(`receiver_init;3)
q)callback:{show "callback"; show x}      // callback received as dictionary
q)receive["callback";"trades";enlist 0]
topic received: trades
Using Kafka Broker : localhost:9092
Using Schema Registry : localhost:8081
q)"callback"
id   | 1i
sym  | "msft"
price| 22.4
size | 786
"callback"
id   | 2i
sym  | "hsbc"
price| 99.4
size | 654

```
