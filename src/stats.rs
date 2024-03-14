use std::ops::AddAssign;

#[derive(Debug, Default, Clone)]
pub struct Stats {
	pub num_pings: u64,
	pub num_gets: u64,
	pub num_sets: u64,

	pub total_ping_time: u64,
	pub total_get_time: u64,
	pub total_set_time: u64,

	pub total_get_size: u64,
	pub total_set_size: u64,
}

impl AddAssign for Stats {
	fn add_assign(&mut self, rhs: Self) {
		*self = Stats {
			num_pings: self.num_pings + rhs.num_pings,
			num_gets: self.num_gets + rhs.num_gets,
			num_sets: self.num_sets + rhs.num_sets,

			total_ping_time: self.total_ping_time + rhs.total_ping_time,
			total_get_time: self.total_get_time + rhs.total_get_time,
			total_set_time: self.total_set_time + rhs.total_set_time,

			total_get_size: self.total_get_size + rhs.total_get_size,
			total_set_size: self.total_set_size + rhs.total_set_size,
		}
	}
}
