use std::env;

use clap::{Arg, Command};
use log::{debug, warn};

use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::util::{get_rdkafka_version, Timeout};
use rdkafka::Message;

fn consume_and_print(brokers: &str, group_id: &str, topics: &[&str]) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "latest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topics.to_vec())
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.poll(Timeout::Never) {
            Some(poll_result) => match poll_result {
                Ok(m) => {
                    match m.payload() {
                        None => {}
                        Some(bytes) => {
                            let parsed = String::from_utf8(bytes.to_vec()).unwrap();
                            // log::info!("{:}", parsed);
                            println!("{:}", parsed);
                            consumer.commit_message(&m, CommitMode::Async).unwrap();
                        }
                    };
                }
                Err(e) => warn!("Kafka error: {}", e),
            },
            None => {}
        };
    }
}

fn main() {
    let matches = Command::new("kafka-consumer")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::new("brokers")
                .short('b')
                .long("brokers")
                .help("Broker list in kafka format")
                .value_name("BOOTSTRAP_BROKERS")
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::new("group-id")
                .short('g')
                .long("group-id")
                .help("Consumer group id")
                .value_name("GROUP_ID")
                .default_value("kafka-console-consumer"),
        )
        .arg(
            Arg::new("topics")
                .short('t')
                .long("topics")
                .help("Topic list.")
                .value_name("TOPICS")
                .num_args(1..)
                .required(true),
        )
        .get_matches();

    if env::var("RUST_LOG").is_err() {
        env::set_var("RUST_LOG", "info");
    }

    env_logger::init();

    let (_version_n, version_s) = get_rdkafka_version();
    debug!("rd_kafka_version: {}", version_s);

    let topics = matches
        .get_many::<String>("topics")
        .unwrap()
        .map(|s| s.to_owned())
        .collect::<Vec<String>>();
    let brokers = matches.get_one::<String>("brokers").unwrap();
    let group_id = matches.get_one::<String>("group-id").unwrap();

    consume_and_print(
        brokers,
        group_id,
        &topics.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
    );
}
