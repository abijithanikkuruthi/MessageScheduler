use std::env;

pub struct Constants {
    pub debug: bool,
    pub scheduler_server_url: String,
    pub request_error_wait_time: u8,
    pub request_count_limit: u8,
}

impl Constants {
    pub fn new() -> Constants {
        Constants {
            debug: env::var("DEBUG").unwrap_or("false".to_string()).to_ascii_lowercase().parse::<bool>().unwrap_or(false),

            scheduler_server_url: env::var("SCHEDULER_SERVER_URL").unwrap_or("http://localhost:8000".to_string()),
            request_error_wait_time: env::var("REQUEST_ERROR_WAIT_TIME").unwrap_or("1".to_string()).parse::<u8>().unwrap_or(1),
            request_count_limit: env::var("REQUEST_COUNT_LIMIT").unwrap_or("3".to_string()).parse::<u8>().unwrap_or(3),
        }
    }
}

impl Clone for Constants {
    fn clone(&self) -> Constants {
        Constants {
            debug: self.debug,

            scheduler_server_url: self.scheduler_server_url.clone(),
            request_error_wait_time: self.request_error_wait_time,
            request_count_limit: self.request_count_limit,
        }
    }
}
