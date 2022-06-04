use crate ::constants;
use crate ::common;
use crate ::multiprocess;

struct Task {
    task : serde_json::Value,
    worker_success : multiprocess::SharedMemory<bool>,
    task_id : i32,
}

impl Clone for Task {
    fn clone(&self) -> Task {
        Task {
            task: self.task.clone(),
            worker_success: self.worker_success.clone(),
            task_id: self.task_id,
        }
    }
}

impl Task {
    pub fn new(task : serde_json::Value, worker_success : multiprocess::SharedMemory<bool>, task_id : i32) -> Task {
        Task {
            task,
            worker_success,
            task_id
        }
    }
    pub fn run(&self) -> multiprocess::JoinHandle {
        let __run = |task_obj : Task| {
            let __task_run = |task_obj : Task| -> Result<(), String> {
                let task = task_obj.task.clone();
                let task_id = task_obj.task_id;


                Ok(())
            };
            let worker_success = task_obj.worker_success.clone();
            match __task_run(task_obj) {
                Ok(_) => {
                    worker_success.set(true);
                },
                Err(e) => {
                    let error_msg = format!("Task.__run: {}", e);
                    common::print_error(error_msg.as_str());
                    worker_success.set(false);
                }
            }
        };
        multiprocess::worker_process(__run, self.clone())
    }
}

pub struct Worker {
    config : serde_json::Value,
    constants : constants::Constants,
    worker_success : multiprocess::SharedMemory<bool>,
    work : serde_json::Value
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
    pub fn new(config : serde_json::Value, constants : constants::Constants, work : serde_json::Value) -> Worker {
        match multiprocess::SharedMemory::new(true) {
            Ok(worker_success) => {
                Worker {
                    config,
                    constants,
                    worker_success,
                    work
                }
            }
            Err(e) => {
                common::print_error(&format!("{}", e));
                panic!("Unable to create worker_success shared memory");
            }
        }
    }
    pub fn run(&self) -> multiprocess::JoinHandle {
        let __run = |worker_obj : Worker| {
            let mut tasks = Vec::new();
            for task_id in 0..worker_obj.config["sm_partitions_per_bucket"].as_i64().unwrap_or(16) {
                let task = Task::new(worker_obj.work.clone(), worker_obj.worker_success.clone(), task_id as i32).run();
                tasks.push(task);
            }
            for task in tasks {
                task.join();
            }
            let mut worker_obj = worker_obj.clone();
            worker_obj.work["status"] = {
                if worker_obj.worker_success.get() {
                    serde_json::json!(worker_obj.config["worker_status_list.done"].as_str().unwrap_or("DONE"))
                } else {
                    common::print_error(&format!("Worker failed"));
                    serde_json::json!(worker_obj.config["worker_status_list.error"].as_str().unwrap_or("ERROR"))
                }
            };
            common::post_worker(&worker_obj.config, &worker_obj.constants, &worker_obj.work);
        };
        multiprocess::worker_process(__run, self.clone())
    }
}