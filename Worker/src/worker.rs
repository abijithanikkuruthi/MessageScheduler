use rdkafka::config::ClientConfig;
use rdkafka::consumer::base_consumer::BaseConsumer;
use rdkafka::message::{Headers, BorrowedHeaders};
use rdkafka::util::Timeout;
use rdkafka::producer::{BaseProducer, BaseRecord};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};

use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use rdkafka::topic_partition_list::TopicPartitionList;

use serde_json::Value as Json;
use serde_json::json;

use std::process::exit;
use std::time::{Duration, Instant};

use crate::constants::Constants;
use crate::common::*;
use crate::multiprocess as mp;

struct Task {
    task : Json,
    config : Json,
    worker_success : mp::SharedMemory<bool>,
    task_id : i32,
}

impl Clone for Task {
    fn clone(&self) -> Task {
        Task {
            task: self.task.clone(),
            config: self.config.clone(),
            worker_success: self.worker_success.clone(),
            task_id: self.task_id,
        }
    }
}

impl Task {
    pub fn new(task : Json, config : Json, worker_success : mp::SharedMemory<bool>, task_id : i32) -> Task {
        Task {
            task,
            config,
            worker_success,
            task_id
        }
    }
    
    pub fn run(&self) -> mp::JoinHandle {
        let __run = | task_obj : Task | {
            let __task_run = | task_obj : Task | -> Result<(), String> {
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
                    let time_diff = (sm_time - chrono::Local::now().naive_local()).num_seconds();
                    
                    if time_diff < -1 * get_number(&config, "sm_miniumum_delay") as i64 {
                        let error_msg = format!("WORKER DELAY: {} secs : {:?}", time_diff, sm_time);
                        print_error(error_msg.as_str());
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

                let __get_hop_count = |headers: &BorrowedHeaders, config: &Json| -> i32 {
                    let key = get_string(&config, "sm_header_message_hopcount_key");
                    let hop_count = __get_header(headers, key.as_str());
                    if hop_count == None {
                        return 0;
                    }
                    hop_count.unwrap().parse::<i32>().unwrap()
                };

                let __get_job_id = |headers: &BorrowedHeaders, config: &Json| -> String {
                    let key = get_string(&config, "sm_header_job_id_key");
                    let job_id = __get_header(headers, key.as_str());
                    if job_id == None {
                        return "".to_string();
                    }
                    job_id.unwrap()
                };

                let task = task_obj.task.clone();
                let task_id = task_obj.task_id;
                let config = task_obj.config;
                let job_ob = get_object(&task, "job");
                let job_name = job_ob["name"].as_str().unwrap();
                let group_id = format!("{}{}_{}", get_string(&config, "sm_consumer_group_prefix"), job_name, task_id);

                let consumer : BaseConsumer = ClientConfig::new()
                    .set("group.id", group_id)
                    .set("bootstrap.servers", get_string(&config, "kafka_server"))
                    .set("enable.auto.commit", "true")
                    .set("auto.offset.reset", "earliest").create().expect("Task.__run(): Consumer creation error");

                let producer : BaseProducer = ClientConfig::new()
                    .set("bootstrap.servers", get_string(&config, "kafka_server"))
                    .create().expect("Task.__run(): Producer creation error");

                let mut tp_list = TopicPartitionList::new();
                tp_list.add_partition(job_name, task_id);
                consumer.assign(&tp_list).expect("Task.__run(): Topic Assignment error");

                loop {
                    match consumer.poll(std::time::Duration::from_secs(get_number(&config, "worker_consumer_timeout"))) {
                        Some(Ok(message)) => {
                            let headers = message.headers().unwrap();

                            let topic_name = __get_topic_name(&config, &headers);
                            if topic_name == None {
                                continue;
                            }
                            let topic_name = topic_name.unwrap();
                            let msg_job_id = __get_job_id(&headers, &config);
                            let msg_hop_count = (__get_hop_count(&headers, &config) + 1) as i32;

                            let msg_headers = headers.detach().add(&get_string(&config, "sm_header_job_id_key"), get_string(&task, "job_id").as_bytes())
                                .add(&get_string(&config, "sm_header_message_hopcount_key"), format!("{}", msg_hop_count).as_bytes())
                                .add(&get_string(&config, "sm_header_worker_timestamp_key"), get_time().as_bytes());

                            let msg_payload = message.payload().unwrap_or_default();
                            let msg_key = message.key().unwrap_or_default();

                            match {
                                let mut retry_count = 0;
                                
                                loop {
                                    let base_record = BaseRecord::to(topic_name.as_str()).payload(msg_payload).key(msg_key).headers(msg_headers.clone()).partition(task_id);

                                    match producer.send(base_record) {
                                        Ok(_) => break Ok(()),
                                        Err((e, record)) => {
                                            retry_count += 1;
                                            if retry_count >= 10000 {
                                                break Err((e, record));
                                            }
                                            std::thread::sleep(std::time::Duration::from_millis(100));
                                        }
                                    }
                                }
                            } {
                                Ok(_) => {
                                    if msg_job_id == get_string(&task, "job_id") {
                                        break;
                                    }
                                },
                                Err(e) => {
                                    let err_msg = format!("Task.__run(): Producer send error: {:?}", e);
                                    print_error(&err_msg);
                                    task_obj.worker_success.set(false);
                                    return Err(err_msg);
                                }
                            }
                        },
                        Some(Err(_)) => {
                            continue;
                        },
                        None => {
                            break;
                        }
                    }
                }

                Ok(())
            };
            let worker_success = task_obj.worker_success.clone();
            match __task_run(task_obj) {
                Ok(_) => {},
                Err(e) => {
                    let error_msg = format!("Task.__run: {}", e);
                    print_error(error_msg.as_str());
                    worker_success.set(false);
                }
            }
            exit(0);
        };
        mp::process(__run, self.clone())
    }
}

pub struct Worker {
    config : Json,
    constants : Constants,
    worker_success : mp::SharedMemory<bool>,
    work : Json
}

impl Clone for Worker {
    fn clone(&self) -> Worker {
        Worker {
            config: self.config.clone(),
            constants: self.constants.clone(),
            worker_success: self.worker_success.clone(),
            work: self.work.clone()
        }
    }
}

impl Worker {
    pub fn new(config : Json, constants : Constants, work : Json) -> Worker {
        match mp::SharedMemory::new(true) {
            Ok(worker_success) => {
                Worker {
                    config,
                    constants,
                    worker_success,
                    work
                }
            }
            Err(e) => {
                print_error(&format!("{}", e));
                panic!("Unable to create worker_success shared memory");
            }
        }
    }
    pub fn run(&self) -> mp::JoinHandle {
        let __run = |worker_obj : Worker| {
            let mut tasks = Vec::new();
            for task_id in 0..worker_obj.config["sm_partitions_per_bucket"].as_i64().unwrap_or(16) {
                let task = Task::new(worker_obj.work.clone(), worker_obj.config.clone(), worker_obj.worker_success.clone(), task_id as i32).run();
                tasks.push(task);
            }
            for task in tasks {
                task.join();
            }
            let mut worker_obj = worker_obj.clone();
            worker_obj.work["status"] = {
                if worker_obj.worker_success.get() {
                    json!(worker_obj.config["worker_status_list.done"].as_str().unwrap_or("DONE"))
                } else {
                    print_error(&format!("Worker failed"));
                    json!(worker_obj.config["worker_status_list.error"].as_str().unwrap_or("ERROR"))
                }
            };
            post_worker(&worker_obj.config, &worker_obj.constants, &worker_obj.work);
            exit(0);
        };
        mp::process(__run, self.clone())
    }
}