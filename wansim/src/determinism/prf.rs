//! Counter-based, domain-separated pseudo-random draws.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CounterPrf {
    master_seed: u64,
    scenario_key: u64,
}

impl CounterPrf {
    pub fn new(master_seed: u64, scenario: &str) -> Self {
        Self {
            master_seed,
            scenario_key: fnv1a64(scenario.as_bytes()),
        }
    }

    pub fn draw_u64(self, domain: &str, component: u64, process: u64, draw_index: u64) -> u64 {
        let mut state = self.master_seed ^ self.scenario_key.rotate_left(17);
        state ^= fnv1a64(domain.as_bytes()).rotate_left(31);
        state ^= component.wrapping_mul(0x9e37_79b9_7f4a_7c15);
        state ^= process.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        state ^= draw_index.wrapping_mul(0x94d0_49bb_1331_11eb);
        splitmix64(state)
    }

    pub fn draw_byte(self, domain: &str, component: u64, process: u64, draw_index: u64) -> u8 {
        self.draw_u64(domain, component, process, draw_index) as u8
    }
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

const fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut index = 0;
    while index < bytes.len() {
        hash ^= bytes[index] as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        index += 1;
    }
    hash
}
