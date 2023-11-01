pub struct Access {
	pub key: String,
	pub value: String,
}

impl Access {
	pub fn new(key: String, value: String) -> Self {
		Access {
			key,
			value,
		}
	}
}
