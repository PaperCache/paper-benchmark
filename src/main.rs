mod access;
mod client;
mod stats;

use std::sync::Arc;
use clap::Parser;
use crossbeam_channel::unbounded;

use kwik::{
	fmt,
	FileReader,
	progress::{Progress, Tag},
	binary_reader::{BinaryReader, SizedChunk},
};

use crate::{
	client::{BenchmarkClient, ClientEvent},
	access::Access,
	stats::Stats,
};

const PING_TEST_COUNT: u64 = 1_000_000;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
	#[arg(long, default_value = "127.0.0.1")]
	host: String,

	#[arg(long, default_value_t = 3145)]
	port: u32,

	#[arg(short, long)]
	trace_path: Option<String>,

	#[arg(short, long, default_value_t = 4)]
	clients: u32,
}

#[tokio::main]
async fn main() {
	let args = Args::parse();

	assert!(args.clients > 0);

	let mut tasks = Vec::new();
	let host = Arc::new(args.host);

	let (sender, receiver) = unbounded::<ClientEvent>();

	println!("Initializing {} client(s)...", args.clients);

	for _ in 0..args.clients {
		let host = host.clone();
		let receiver = receiver.clone();

		let task = tokio::spawn(async move {
			let mut client = BenchmarkClient::new(&host, args.port, receiver)
				.expect("Could not create client.");

			client.run()
		});

		tasks.push(task);
	}

	println!("\nPerforming {} pings...", fmt::number(PING_TEST_COUNT));

	for _ in 0..PING_TEST_COUNT {
		sender.send(ClientEvent::Ping)
			.expect("Could not send ping to client.");
	}

	if let Some(trace_path) = &args.trace_path {
		let reader = BinaryReader::<Access>::new(trace_path)
			.expect("Invalid trace path.");

		println!("\nProcessing {} accesses...", fmt::number(reader.size() / Access::size() as u64));

		let mut progress = Progress::new(reader.size(), &[
			Tag::Tps,
			Tag::Eta,
			Tag::Time,
		]);

		for access in reader {
			sender.send(ClientEvent::Access(access))
				.expect("Could not send access to client.");

			progress.tick(Access::size());
		}
	}

	drop(sender);

	println!();

	let mut stats = Stats::default();

	println!("Waiting for clients to handle events...");

	for task in tasks {
		stats += task.await
			.expect("Could not terminate client")
			.expect("Error executing client requests");
	}

	println!();
	println!("Total PINGs:\t{}", fmt::number(stats.num_pings));
	println!("Total GETs:\t{}", fmt::number(stats.num_gets));
	println!("Total SETs:\t{}", fmt::number(stats.num_sets));

	println!();
	print_avg_size("GET", stats.num_gets, stats.total_get_size);
	print_avg_size("SET", stats.num_sets, stats.total_set_size);

	println!();
	print_stat_rate("PING", stats.num_pings, stats.total_ping_time);
	print_stat_rate("GET", stats.num_gets, stats.total_get_time);
	print_stat_rate("SET", stats.num_sets, stats.total_set_time);

	println!();
	print_stat_time("PING", stats.num_pings, stats.total_ping_time);
	print_stat_time("GET", stats.num_gets, stats.total_get_time);
	print_stat_time("SET", stats.num_sets, stats.total_set_time);
}

fn print_avg_size(label: &str, num: u64, total_size: u64) {
	if num == 0 || total_size == 0 {
		return;
	}

	let avg_size = total_size / num;

	println!(
		"Avg {label} size:\t{} ({} B)",
		fmt::memory(avg_size, Some(2)),
		avg_size,
	);
}

fn print_stat_rate(label: &str, num: u64, total_time: u64) {
	if num == 0 || total_time == 0 {
		return;
	}

	let rate = num as f64 / (total_time / 1_000_000) as f64;

	println!("{label}s per sec:\t{}", fmt::number(rate as u64));
}

fn print_stat_time(label: &str, num: u64, total_time: u64) {
	if num == 0 || total_time == 0 {
		return;
	}

	println!(
		"Time per {label}:\t{} ({}s)",
		(total_time as f64 / num as f64).round(),
		std::char::from_u32(0x03bc).unwrap(),
	);
}
