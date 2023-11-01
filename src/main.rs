mod access;

use clap::Parser;

use kwik::{
	fmt,
	progress::{Progress, Tag},
};

use paper_client::PaperClient;
use crate::access::Access;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
	#[arg(short, long, default_value = "127.0.0.1")]
	host: String,

	#[arg(short, long, default_value_t = 3145)]
	port: u32,

	#[arg(short, long, default_value_t = 104857600)]
	size: u64,

	#[arg(short, long, default_value_t = 1_000_000)]
	num_accesses: u64,

	#[arg(short, long, default_value_t = 3)]
	data_size: u32,
}

fn main() {
	let args = Args::parse();

	let Ok(mut client) = PaperClient::new(&args.host, args.port) else {
		println!("Could not connect to host.");
		return;
	};

	client.resize(args.size).unwrap();

	let accesses = init_sets(&args);

	println!("Processing {} sets", fmt::number(args.num_accesses));

	let mut progress = Progress::new(args.num_accesses, &[
		Tag::Tps,
		Tag::Eta,
		Tag::Time,
	]);

	for access in &accesses {
		client.set(&access.key, &access.value, Some(5)).unwrap();
		progress.tick(1);
	}

	println!("\nProcessing {} gets", fmt::number(args.num_accesses));

	let mut progress = Progress::new(args.num_accesses, &[
		Tag::Tps,
		Tag::Eta,
		Tag::Time,
	]);

	for access in &accesses {
		client.get(&access.key).unwrap();
		progress.tick(1);
	}
}

fn init_sets(args: &Args) -> Vec<Access> {
	let mut accesses = Vec::<Access>::new();

	for i in 0..args.num_accesses {
		let value: String = std::iter::repeat('0')
			.take(args.data_size as usize)
			.collect();

		let access = Access::new(
			format!("{}", i),
			value,
		);

		accesses.push(access);
	}

	accesses
}
