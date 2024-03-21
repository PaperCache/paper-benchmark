use std::time::{Instant, Duration};
use crossbeam_channel::Receiver;
use paper_client::{PaperClient, PaperClientError};

use crate::{
	access::{Access, Command},
	stats::Stats,
};

pub type ClientReceiver = Receiver<ClientEvent>;

pub struct BenchmarkClient {
	client: PaperClient,
	events: ClientReceiver,
	stats: Stats,
}

pub enum ClientEvent {
	Ping,
	Access(Access),
}

impl BenchmarkClient {
	pub fn new(
		host: &str,
		port: u32,
		auth: Option<String>,
		events: ClientReceiver,
	) -> Result<Self, PaperClientError> {
		let mut client = PaperClient::new(host, port)?;

		if let Some(token) = &auth {
			client.auth(token)?;
		}

		client.wipe()?;

		let benchmark_client = BenchmarkClient {
			client,
			events,
			stats: Stats::default(),
		};

		Ok(benchmark_client)
	}

	pub fn run(&mut self) -> Result<Stats, PaperClientError> {
		let max_wait = Duration::from_secs(5);

		while let Ok(event) = self.events.recv_timeout(max_wait) {
			match event {
				ClientEvent::Ping => self.handle_ping()?,
				ClientEvent::Access(access) => self.handle_access(access)?,
			}
		}

		Ok(self.stats.clone())
	}

	fn handle_ping(&mut self) -> Result<(), PaperClientError> {
		let start_time = Instant::now();

		self.client.ping()?;
		self.stats.store_ping_time(start_time.elapsed());

		Ok(())
	}

	fn handle_access(&mut self, access: Access) -> Result<(), PaperClientError> {
		match access.command {
			Command::Get => {
				let start_time = Instant::now();

				if let Err(err) = self.client.get(&access.key) {
					if !matches!(err, PaperClientError::CacheError(_)) {
						return Err(err);
					}
				} else {
					self.stats.store_get_size(access.value.len() as u64);
				}

				self.stats.store_get_time(start_time.elapsed());
			},

			Command::Set => {
				let start_time = Instant::now();

				self.client.set(&access.key, &access.value, access.ttl)?;

				self.stats.store_set_time(start_time.elapsed());
				self.stats.store_set_size(access.value.len() as u64);
			},
		}

		Ok(())
	}
}
