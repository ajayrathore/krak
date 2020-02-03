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
q)pub : `libkrak 2:(`publish;4)
q)
q)t:([]id:10?100i;sym:string 10?`2;price:10?100f;size:10?100)  // symbols are not supported, pass as strings
q)3#t
id  sym  price    size
----------------------
77  "ci" 8.388858 12
30  "hk" 19.59907 10
17  "ae" 37.5638  1
q)
q)pub["mykey"; t; `int$count t; string cols t] // 4 params - key, table, row count, cols
Using Kafka Broker : localhost:9092
publish status = [ProduceConfirm { topic: "trades", partition_confirms: [ProducePartitionConfirm { offset: Ok(3212), partition: 0 }] }]
```

Consuming From kafka broker
```
q)receive : `libkrak 2:(`receiver_init;3)
q)callback:{[k;v] show "callback"; show k; show v}  // callback params - key(string), value(dictionary)
q)receive["callback";"trades";enlist 0]  // params - callback func name, topic, list of partitions
topic received: trades
Using Kafka Broker : localhost:9092
Using Schema Registry : localhost:8081
q)"callback"
"mykey"
sid  | 88i
sym  | "pl"
price| 78.5033
size | 12
```

Publishing schema from kdb
```
q)post : `libkrak 2:(`post_schema;2)
q)
q)json:.j.j `type`name`fields!(`record;`quote;flip(`name`type!(`id`sym`bid`ask;`int`string`double`double)))
q)
q)post["subjects/test-value/versions"; json]
Using Schema Registry : localhost:8081
schema posting to localhost:8081/subjects/test-value/versions
Schema posted, Received Id: Ok((Record { name: Name { name: "quote", namespace: None, aliases: None }, doc: None, fields: [RecordField { name: "id", doc: None, default: None, schema: Int, order: Ascending, position: 0 }, RecordField { name: "sym", doc: None, default: None, schema: String, order: Ascending, position: 1 }, RecordField { name: "bid", doc: None, default: None, schema: Double, order: Ascending, position: 2 }, RecordField { name: "ask", doc: None, default: None, schema: Double, order: Ascending, position: 3 }], lookup: {"sym": 1, "bid": 2, "id": 0, "ask": 3} }, 3))
```
