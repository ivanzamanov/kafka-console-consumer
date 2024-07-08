use clap::{App, Arg};
use env_logger::Env;
use log::{info, warn};

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
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
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
                        None => {},
                        Some(bytes) => {
                            let parsed = String::from_utf8(bytes.to_vec()).unwrap();
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
    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Simple command line consumer")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("example_consumer_group_id"),
        )
        .arg(
            Arg::with_name("log-conf")
                .long("log-conf")
                .help("Configure the logging format (example: 'rdkafka=trace')")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("topics")
                .short("t")
                .long("topics")
                .help("Topic list")
                .takes_value(true)
                .multiple(true)
                .required(true),
        )
        .get_matches();

    let env = Env::default();
    env_logger::init_from_env(env);

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let topics = matches.values_of("topics").unwrap().collect::<Vec<&str>>();
    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    consume_and_print(brokers, group_id, &topics);
}
