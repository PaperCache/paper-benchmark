mod access;
mod client;
mod stats;

use std::sync::Arc;
use clap::Parser;
use crossbeam_channel::bounded;

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

	let host = Arc::new(args.host);

	let (sender, receiver) = bounded::<ClientEvent>(args.clients as usize);

	println!("Initializing {} client(s)", args.clients);

	let clients = (0..args.clients)
		.map(|_| {
			let host = host.clone();
			let receiver = receiver.clone();

			BenchmarkClient::new(&host, args.port, receiver)
				.expect("Could not create client.")
		})
		.collect::<Vec<BenchmarkClient>>();

	let tasks = clients
		.into_iter()
		.map(|mut client| tokio::spawn(async move {
			client.run()
		}))
		.collect::<Vec<_>>();

	println!("\nPerforming {} pings", fmt::number(PING_TEST_COUNT));

	let mut progress = Progress::new(PING_TEST_COUNT, &[
		Tag::Tps,
		Tag::Eta,
		Tag::Time,
	]);

	for _ in 0..PING_TEST_COUNT {
		sender.send(ClientEvent::Ping)
			.expect("Could not send ping to client.");

		progress.tick(1);
	}

	if let Some(trace_path) = &args.trace_path {
		let reader = BinaryReader::<Access>::new(trace_path)
			.expect("Invalid trace path.");

		println!("\nProcessing {} accesses", fmt::number(reader.size() / Access::size() as u64));

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

	let mut stats = Stats::default();

	for task in tasks {
		stats += task.await
			.expect("Could not terminate client")
			.expect("Error executing client requests");
	}

	stats.print_ping_stats();
	stats.print_get_stats();
	stats.print_set_stats();
}
