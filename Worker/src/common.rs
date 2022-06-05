use chrono;
use serde_json;

use crate::constants;

#[allow(dead_code)]
enum Colors {
    Header,
    OkBlue,
    OkCyan,
    OkGreen,
    Warning,
    Error,
    Endc,
    Bold,
    Underline,
}

impl Colors {
    fn as_str(&self) -> &'static str {
        match self {
            Colors::Header => "\x1b[95m",
            Colors::OkBlue => "\x1b[94m",
            Colors::OkCyan => "\x1b[96m",
            Colors::OkGreen => "\x1b[92m",
            Colors::Warning => "\x1b[93m",
            Colors::Error => "\x1b[91m",
            Colors::Endc => "\x1b[0m",
            Colors::Bold => "\x1b[1m",
            Colors::Underline => "\x1b[4m",
        }
    }
}

pub fn get_time() -> String {
    let now = chrono::Local::now();
    now.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub fn print_header(message: &str) {
    println!("{}[{}] {}{}", Colors::Bold.as_str(), get_time(), message, Colors::Endc.as_str());
}

pub fn print_ok(message: &str) {
    println!("{}[{}] {}{}", Colors::OkGreen.as_str(), get_time(), message, Colors::Endc.as_str());
}

pub fn print_bold(message: &str) {
    println!("{}[{}] {}{}", Colors::Bold.as_str(), get_time(), message, Colors::Endc.as_str());
}

pub fn print_warning(message: &str) {
    println!("{}[{}] {}{}", Colors::Warning.as_str(), get_time(), message, Colors::Endc.as_str());
}

pub fn print_error(message: &str) {
    println!("{}[{}] {}{}", Colors::Error.as_str(), get_time(), message, Colors::Endc.as_str());
}

pub fn generate_uuid() -> String {
    use uuid::Uuid;
    let uuid = Uuid::new_v4();
    uuid.as_hyphenated().to_string()
}

fn get_response(url: &str, retries: u8, retry_delay: u8) -> Result<reqwest::blocking::Response, String> {
    use reqwest::blocking::get;

    let mut tries = 0;

    loop {
        match get(url) {
            Ok(response) => {
                if response.status().is_success() {
                    return Ok(response);
                }
            }
            Err(e) => {
                print_error(&format!("{}", e));
                tries += 1;
                if tries >= retries {
                    break;
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(retry_delay as u64));
    }
    Err(format!("Unable to get response from {}", url))
}

fn post_response(url: &str, body: &str, retries: u8, retry_delay: u8) -> Result<reqwest::blocking::Response, String> {
    use reqwest::blocking::Client;

    let mut tries = 0;

    loop {
        match Client::new().post(url).body(body.to_string()).send() {
            Ok(response) => {
                if response.status().is_success() {
                    return Ok(response);
                }
            }
            Err(e) => {
                print_error(&format!("{}", e));
                tries += 1;
                if tries >= retries {
                    break;
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(retry_delay as u64));
    }
    Err(format!("Unable to post response to {}", url))
}

fn get_json_response(url: &str, retries: u8, retry_delay: u8) -> serde_json::Value {
    loop {
        match get_response(url, retries, retry_delay) {
            Ok(response) => {
                return response.json().unwrap_or(serde_json::json!({}));
            }
            Err(e) => {
                print_error(&format!("{}", e));
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(retry_delay as u64));
    }
}

fn post_json_response(url: &str, body: &str, retries: u8, retry_delay: u8) -> serde_json::Value {
    loop {
        match post_response(url, body, retries, retry_delay) {
            Ok(response) => {
                return response.json().unwrap_or(serde_json::json!({}));
            }
            Err(e) => {
                print_error(&format!("{}", e));
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(retry_delay as u64));
    }
}

pub fn get_config(constants : &constants::Constants) -> serde_json::Value {
    let mut config = get_json_response(&format!("{}/config", constants.scheduler_server_url), constants.request_count_limit, constants.request_error_wait_time);
    config["last_updated"] = serde_json::json!(get_time());
    config
}

pub fn get_worker(config : &serde_json::Value, constants : &constants::Constants) -> serde_json::Value {
    let mut worker = get_json_response(config["worker_url"].as_str().unwrap_or(""), constants.request_count_limit, constants.request_error_wait_time);
    worker["last_updated"] = serde_json::json!(get_time());
    worker
}

pub fn post_worker(config : &serde_json::Value, constants : &constants::Constants, worker : &serde_json::Value) -> serde_json::Value {
    let mut worker = post_json_response(config["worker_url"].as_str().unwrap_or(""), &worker.to_string(), constants.request_count_limit, constants.request_error_wait_time);
    worker["last_updated"] = serde_json::json!(get_time());
    worker
}

pub fn get_number(json : &serde_json::Value, key : &str) -> u64 {
    json[key].as_u64().unwrap_or(0)
}

pub fn get_float(json : &serde_json::Value, key : &str) -> f64 {
    json[key].as_f64().unwrap_or(0.0)
}

pub fn get_string(json : &serde_json::Value, key : &str) -> String {
    json[key].as_str().unwrap_or("").to_string()
}

pub fn get_list(json : &serde_json::Value, key : &str) -> Vec<serde_json::Value> {
    let default : Vec<serde_json::Value> = Vec::new();
    json[key].as_array().unwrap_or(&default).to_vec()
}

pub fn get_object(json : &serde_json::Value, key : &str) -> serde_json::Map<String, serde_json::Value> {
    json[key].as_object().unwrap_or(&serde_json::Map::new()).to_owned()
}

pub fn u8_to_str(u8_list : &[u8]) -> String {
    let mut string = String::new();
    for u8_ in u8_list {
        string.push(*u8_ as char);
    }
    string
}