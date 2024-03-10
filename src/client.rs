use std::time::Instant;
use crossbeam_channel::Receiver;
use paper_client::{PaperClient, PaperClientError};

use crate::{
	access::{Access, Command},
	stats::Stats,
};

pub type ClientReceiver = Receiver<Access>;

pub struct BenchmarkClient {
	client: PaperClient,
	accesses: ClientReceiver,
	stats: Stats,
}

impl BenchmarkClient {
	pub fn new(
		host: &str,
		port: u32,
		accesses: ClientReceiver,
	) -> Result<Self, PaperClientError> {
		let mut client = PaperClient::new(host, port)?;
		client.wipe()?;

		let benchmark_client = BenchmarkClient {
			client,
			accesses,
			stats: Stats::default(),
		};

		Ok(benchmark_client)
	}

	pub fn run(&mut self) -> Result<Stats, PaperClientError> {
		while let Ok(access) = self.accesses.recv() {
			match access.command {
				Command::Get => {
					let start_time = Instant::now();

					self.client.get(&access.key)?;

					self.stats.total_get_time += start_time
						.elapsed()
						.as_micros() as u64;

					self.stats.num_gets += 1;
				},

				Command::Set => {
					let start_time = Instant::now();

					self.client.set(&access.key, &access.value, access.ttl)?;

					self.stats.total_set_time += start_time
						.elapsed()
						.as_micros() as u64;

					self.stats.num_sets += 1;
				},
			}
		}

		Ok(self.stats.clone())
	}
}
