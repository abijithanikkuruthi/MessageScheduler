use serde_json;

mod common;
mod constants;
mod worker;
mod multiprocess;

struct WorkerProcess {
    constants: constants::Constants,
    config : serde_json::Value,
}

impl Clone for WorkerProcess {
    fn clone(&self) -> WorkerProcess {
        WorkerProcess {
            constants: self.constants.clone(),
            config: self.config.clone(),
        }
    }
}

impl WorkerProcess {
    pub fn new(constants: constants::Constants, config: serde_json::Value) -> WorkerProcess {
        WorkerProcess {
            constants,
            config
        }
    }
    pub fn run(&self) -> multiprocess::JoinHandle {
        let __run = |wp: WorkerProcess| {
            let mut config = wp.config.clone();
            let constants = wp.constants.clone();
            config["worker_id"] = serde_json::json!(common::generate_uuid());
            config["worker_url"] = serde_json::json!(format!("{}/worker/{}", constants.scheduler_server_url, config["worker_id"].as_str().unwrap_or("")));
            
            loop {
                let config_clone = config.clone();
                let constants_clone = constants.clone();
                let work_object = common::get_worker(&config_clone, &constants_clone);
                let work_object_status = work_object["status"].as_str().unwrap_or("ERROR");

                // READY state - Wait for a task
                if work_object_status == config["worker_status_list.ready"].as_str().unwrap_or("READY") {
                    let config_clone = config.clone();
                    let constants_clone = constants.clone();
                    let work_object = common::post_worker(&config_clone, &constants_clone, &work_object);
                    let work_object_status = work_object["status"].as_str().unwrap_or("ERROR");
                    if work_object_status == config["worker_status_list.working"].as_str().unwrap_or("WORKING") {
                        worker::Worker::new(config.clone(), constants.clone(), work_object.clone()).run();
                    }
                }
                // WORKING state - do nothing
                else if work_object_status == config["worker_status_list.working"].as_str().unwrap_or("WORKING") {
                    // do nothing
                }

                // DONE state - Change to READY state and wait for a task
                // ERROR state - Change to READY state and wait for a task
                else if (work_object_status == config["worker_status_list.done"].as_str().unwrap_or("DONE")) ||
                        (work_object_status == config["worker_status_list.error"].as_str().unwrap_or("ERROR")) {
                    let mut work_object = work_object.clone();
                    work_object["status"] = serde_json::json!(config["worker_status_list.ready"].as_str().unwrap_or("READY"));
                    let config_clone = config.clone();
                    let constants_clone = constants.clone();
                    let work_object = common::post_worker(&config_clone, &constants_clone, &work_object);
                    let work_object_status = work_object["status"].as_str().unwrap_or("ERROR");
                    if work_object_status == config["worker_status_list.working"].as_str().unwrap_or("WORKING") {
                        worker::Worker::new(config.clone(), constants.clone(), work_object.clone()).run();
                    }
                }
                // sleep
                let sleep_time = match work_object["status"].as_str().unwrap_or("ERROR") {
                    "WORKING" => {
                        config["worker_working_poll_freq"].as_u64().unwrap_or(10)
                    },
                    _ => {
                        config["worker_ready_poll_freq"].as_u64().unwrap_or(1)
                    }
                };
                std::thread::sleep(std::time::Duration::from_secs(sleep_time));
            }
        };
        multiprocess::worker_process(__run, self.clone())
    }
}
fn main() {
    let constants = constants::Constants::new();
    let config = common::get_config(&constants);
    
    // create worker processes
    let mut worker_processes = Vec::new();
    for _ in 0..config["worker_process_count"].as_u64().unwrap_or(1) {
        worker_processes.push(WorkerProcess::new(constants.clone(), config.clone()).run());
    }
    common::print_ok(format!("Worker Service Started in {} processes", config["worker_process_count"].as_u64().unwrap_or(1)).as_str());
    worker_processes.pop().unwrap().join();
}
