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
	client::BenchmarkClient,
	access::Access,
	stats::Stats,
};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
	#[arg(long, default_value = "127.0.0.1")]
	host: String,

	#[arg(long, default_value_t = 3145)]
	port: u32,

	#[arg(short, long)]
	trace_path: String,

	#[arg(short, long, default_value_t = 2)]
	clients: u32,
}

#[tokio::main]
async fn main() {
	let args = Args::parse();

	assert!(args.clients > 0);

	let mut tasks = Vec::new();
	let host = Arc::new(args.host);

	let (sender, receiver) = unbounded::<Access>();

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

	let reader = BinaryReader::<Access>::new(&args.trace_path)
		.expect("Invalid trace path.");

	println!("\nProcessing {} accesses", fmt::number(reader.size() / Access::size() as u64));

	let mut progress = Progress::new(reader.size(), &[
		Tag::Tps,
		Tag::Eta,
		Tag::Time,
	]);

	for access in reader {
		sender.send(access).expect("Could not send access to client.");
		progress.tick(Access::size());
	}

	drop(sender);

	println!();

	let mut stats = Stats::default();

	for (i, task) in tasks.into_iter().enumerate() {
		println!("Processing stats of client {}...", i + 1);

		stats += task.await
			.expect("Could not terminate client")
			.expect("Error executing client requests");
	}

	let get_rate = stats.num_gets as f64 / (stats.total_get_time / 1_000_000) as f64;
	let set_rate = stats.num_sets as f64 / (stats.total_set_time / 1_000_000) as f64;

	println!();
	println!("GET accesses/sec: {}", fmt::number(get_rate as u64));
	println!("SET accesses/sec: {}", fmt::number(set_rate as u64));

	println!();

	println!(
		"GET Time per access: {} ({}s)",
		(stats.total_get_time as f64 / stats.num_gets as f64).round(),
		std::char::from_u32(0x03bc).unwrap(),
	);

	println!(
		"SET Time per access: {} ({}s)",
		(stats.total_set_time as f64 / stats.num_sets as f64).round(),
		std::char::from_u32(0x03bc).unwrap(),
	);
}
