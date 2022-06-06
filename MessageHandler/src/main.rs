use serde_json::Value as Json;

mod common;
mod constants;
mod multiprocess;
mod messagehandler;

use common::*;
use constants::Constants;
use multiprocess as mp;
use messagehandler::MessageHandler;

struct MessageHandlerProcess {
    config: Json,
}

impl MessageHandlerProcess {
    pub fn new(config: &Json) -> MessageHandlerProcess {
        MessageHandlerProcess {
            config: config.clone(),
        }
    }
    pub fn run(&self) -> mp::JoinHandle {
        let __run = | config : &Json | {
            MessageHandler::new(&config).run();
        };
        mp::process(__run, &self.config)
    }
}

fn main() {
    let constants = Constants::new();
    let config = get_config(&constants);
    
    // create message handler processes
    let mut mh_process_list = Vec::new();
    for _ in 0..get_number(&config, "sm_mh_process_count") {
        mh_process_list.push(MessageHandlerProcess::new(&config).run());
    }
    print_ok(format!("MessageHandler started in {} processes.", get_number(&config, "sm_mh_process_count")).as_str());

    mh_process_list.pop().unwrap().join();
}
