//! Implements packet drop strategies for the scheduler.

use clap::ValueEnum;
use rand::SeedableRng;
use rand::distr::Distribution;
use rand::distr::Uniform;
use rand::rngs::SmallRng;
use serde::Deserialize;

#[allow(unused)]
/// Capacity unit for the packet drop strategy.
pub enum CapacityUnit {
    Bytes, // pending future implementation
    Packets,
}

/// The packet drop strategy.
#[derive(Clone, Copy, Debug, Deserialize, ValueEnum, Default)]
pub enum DropStrategy {
    #[default]
    TailDrop,
    Red,
}

/// Defines the interface for all packet drop strategies.
pub trait PacketDrop {
    fn should_drop(&mut self, packet_size: usize, byte_size: usize, queue_length: usize) -> bool;
}

/// TailDrop is a packet drop strategy that drops packets when the buffer is full.
pub struct TailDrop {
    capacity: usize, // 0 for unlimited
    capacity_unit: CapacityUnit,
}

impl TailDrop {
    pub fn new(capacity: usize, capacity_unit: CapacityUnit) -> TailDrop {
        TailDrop {
            capacity,
            capacity_unit,
        }
    }
}

impl PacketDrop for TailDrop {
    fn should_drop(&mut self, packet_size: usize, byte_size: usize, queue_length: usize) -> bool {
        match self.capacity_unit {
            CapacityUnit::Bytes => self.capacity > 0 && byte_size + packet_size > self.capacity,
            CapacityUnit::Packets => self.capacity > 0 && queue_length + 1 > self.capacity,
        }
    }
}

/// Random Early Detection (RED), as defined in RFC 2309.
pub struct Red {
    capacity: usize, // 0 for unlimited
    capacity_unit: CapacityUnit,
    min_threshold: f64,
    max_threshold: f64,
    max_probability: f64,
    weight_factor: u32,
    avg_queue_length: usize,
    rng: SmallRng,
}

impl Red {
    pub fn new(
        capacity: usize,
        capacity_unit: CapacityUnit,
        min_threshold: f64,
        max_threshold: f64,
        max_probability: f64,
    ) -> Red {
        let rng = SmallRng::from_os_rng();

        Red {
            capacity,
            capacity_unit,
            min_threshold,
            max_threshold,
            max_probability,
            weight_factor: 9,
            avg_queue_length: 0,
            rng,
        }
    }
}

impl PacketDrop for Red {
    fn should_drop(&mut self, packet_size: usize, byte_size: usize, queue_length: usize) -> bool {
        if self.capacity == 0 {
            return false; // unlimited
        }

        let alpha = 1 / usize::pow(2, self.weight_factor);
        self.avg_queue_length = self.avg_queue_length * (1 - alpha) + queue_length * alpha;

        // drops the packet if the capacity of the queue is exceeded
        let queue_overflow = match self.capacity_unit {
            CapacityUnit::Bytes => self.capacity > 0 && byte_size + packet_size > self.capacity,
            CapacityUnit::Packets => self.capacity > 0 && queue_length + 1 > self.capacity,
        };

        // drops the packet if the average queue length exceeds the max_threshold
        let threshold_overflow = match self.capacity_unit {
            CapacityUnit::Bytes => {
                if byte_size + packet_size
                    > (self.max_threshold * self.capacity as f64).floor() as usize
                {
                    let drop_probability = Uniform::new(0.0, 1.0).unwrap().sample(&mut self.rng);

                    drop_probability <= self.max_probability
                } else {
                    false
                }
            }
            CapacityUnit::Packets => {
                if queue_length + 1 > (self.max_threshold * self.capacity as f64).floor() as usize {
                    let drop_probability = Uniform::new(0.0, 1.0).unwrap().sample(&mut self.rng);

                    drop_probability <= self.max_probability
                } else {
                    false
                }
            }
        };

        let threshold_normal = match self.capacity_unit {
            CapacityUnit::Bytes => {
                if byte_size + packet_size
                    > (self.min_threshold * self.capacity as f64).floor() as usize
                {
                    let probability = f64::max(
                        0.0,
                        self.avg_queue_length as f64 - self.min_threshold * self.capacity as f64,
                    ) / (self.max_threshold - self.min_threshold)
                        * self.capacity as f64
                        * self.max_probability;
                    let drop_probability = Uniform::new(0.0, 1.0).unwrap().sample(&mut self.rng);

                    drop_probability <= probability
                } else {
                    false
                }
            }
            CapacityUnit::Packets => {
                if queue_length + 1 > (self.min_threshold * self.capacity as f64).floor() as usize {
                    let probability = f64::max(
                        0.0,
                        self.avg_queue_length as f64 - self.min_threshold * self.capacity as f64,
                    ) / (self.max_threshold - self.min_threshold)
                        * self.capacity as f64
                        * self.max_probability;
                    let drop_probability = Uniform::new(0.0, 1.0).unwrap().sample(&mut self.rng);

                    drop_probability <= probability
                } else {
                    false
                }
            }
        };

        queue_overflow || threshold_overflow || threshold_normal
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tail_drop_allows_when_capacity_unlimited() {
        let mut dropper = TailDrop::new(0, CapacityUnit::Packets);

        assert!(!dropper.should_drop(128, 0, 0));
        assert!(!dropper.should_drop(128, 0, 10));
    }

    #[test]
    fn tail_drop_rejects_when_packet_capacity_exceeded() {
        let mut dropper = TailDrop::new(2, CapacityUnit::Packets);

        assert!(dropper.should_drop(128, 0, 2));
        assert!(!dropper.should_drop(128, 0, 1));
    }

    #[test]
    fn tail_drop_rejects_when_byte_capacity_exceeded() {
        let mut dropper = TailDrop::new(256, CapacityUnit::Bytes);

        assert!(dropper.should_drop(200, 100, 1));
        assert!(!dropper.should_drop(100, 100, 1));
    }

    #[test]
    fn red_accepts_when_capacity_unlimited() {
        let mut dropper = Red::new(0, CapacityUnit::Packets, 0.5, 0.75, 0.2);

        assert!(!dropper.should_drop(128, 0, 100));
    }

    #[test]
    fn red_drops_when_queue_overflows_capacity() {
        let mut dropper = Red::new(3, CapacityUnit::Packets, 0.5, 0.75, 0.2);

        assert!(dropper.should_drop(128, 0, 3));
    }

    #[test]
    fn red_does_not_drop_below_min_threshold() {
        let mut dropper = Red::new(10, CapacityUnit::Packets, 0.5, 0.75, 0.2);

        // queue length remains below min threshold (0.5 * capacity = 5)
        for queue_length in 0..5 {
            assert!(
                !dropper.should_drop(128, 0, queue_length),
                "Packets below minimum threshold should not be dropped (queue length {queue_length})"
            );
        }
    }
}
