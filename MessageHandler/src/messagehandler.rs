use rdkafka::config::ClientConfig;
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::message::{Headers, BorrowedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use futures::executor;

use rdkafka::consumer::Consumer;
use rdkafka::message::Message;

use serde_json::Value as Json;

use crate ::common::*;

pub struct MessageHandler {
    config: Json,
}

impl MessageHandler {
    pub fn new(config: &Json) -> MessageHandler {
        MessageHandler {
            config: config.clone(),
        }
    }
    pub fn run(&self) {
        let __run = | config : &Json | -> Result<(), String> { 
            let __get_topic_name = |config: &Json, headers: &BorrowedHeaders| -> Option<String> {
                let mut sm_topic_name = None;
                let mut sm_time = None;
        
                for i in 0..headers.count() {
                    let header = headers.get(i).unwrap_or_default();
                    if header.0 == "topic" {
                        sm_topic_name = Some(u8_to_str(header.1));
                    } else if header.0 == "time" {
                        sm_time = Some(u8_to_str(header.1));
                    }
                }
                if sm_topic_name == None {
                    return None;
                }
                if sm_time == None {
                    return sm_topic_name;
                }
        
                let sm_time = sm_time.unwrap();
                let sm_time = chrono::NaiveDateTime::parse_from_str(sm_time.as_str(), get_string(&config, "sm_time_format").as_str()).unwrap();
                let time_diff = (sm_time.time() - chrono::Local::now().time()).num_seconds();
                
                if time_diff < -1 * get_number(&config, "sm_miniumum_delay") as i64 {
                    let error_msg = format!("MessageHandler DELAY: {} secs : {:?}", time_diff, sm_time);
                    print_warning(error_msg.as_str());
                    return sm_topic_name;
                }
                if time_diff < get_number(&config, "sm_miniumum_delay") as i64 {
                    return sm_topic_name;
                }
                let bucket_list = get_list(&config, "bucket_object_list").into_iter().rev().collect::<Vec<_>>();
                for bucket in bucket_list.iter() {
                    let b_time = (1.0 + get_float(&config, "sm_bucket_process_time_fraction")) * get_float(&bucket, "lower");
                    if time_diff > b_time as i64 {
                        return Some(get_string(bucket, "name"));
                    }
                }
                sm_topic_name
            };

            let __get_header = |headers: &BorrowedHeaders, key: &str| -> Option<String> {
                for i in 0..headers.count() {
                    let header = headers.get(i).unwrap_or_default();
                    if header.0 == key {
                        return Some(u8_to_str(header.1));
                    }
                }
                None
            };

            let consumer : BaseConsumer = ClientConfig::new()
                    .set("group.id", get_string(&config, "sm_mh_consumer_group_id"))
                    .set("bootstrap.servers", get_string(&config, "kafka_server"))
                    .set("enable.auto.commit", "true")
                    .set("auto.offset.reset", "earliest").create().expect("MessageHandler.__run(): Consumer creation error");

            let producer : &FutureProducer = &ClientConfig::new()
                .set("bootstrap.servers", get_string(&config, "kafka_server"))
                .create().expect("MessageHandler.__run(): Producer creation error");

            consumer.subscribe(&[get_string(&config, "sm_topic").as_str()]).expect("MessageHandler.__run(): Subscribe error");
            
            loop {
                match consumer.poll(std::time::Duration::from_secs(get_number(&config, "worker_consumer_timeout"))) {
                    Some(message) => {
                        let message = message.expect("MessageHandler.__run(): Message Parse Error");
                        
                        let headers = message.headers().unwrap();

                        let topic_name = __get_topic_name(&config, &headers);
                        if topic_name == None {
                            continue;
                        }
                        let topic_name = topic_name.unwrap();
                        
                        let msg_headers = headers.detach().add(&get_string(&config, "sm_header_mh_timestamp_key"), get_time().as_bytes());

                        match executor::block_on(producer.send(
                            FutureRecord::to(&topic_name)
                                .payload(message.payload().unwrap_or_default())
                                .key(message.key().unwrap_or_default())
                                .headers(msg_headers),
                            std::time::Duration::from_secs(0),
                        )) {
                            Ok(_) => {},
                            Err(e) => {
                                let err_msg = format!("MessageHandler.__run(): Producer send error: {:?}", e);
                                print_error(&err_msg);
                                return Err(err_msg);
                            }
                        }
                    },
                    None => {
                        continue;
                    }
                }
            }
        };

        loop {
            match __run(&self.config) {
                Ok(_) => {},
                Err(e) => {
                    print_error(&format!("MessageHandler::run(): {}", e));
                    print_error("MessageHandler::run() failed. Trying to restart the process now...");
                }
            }
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
}
