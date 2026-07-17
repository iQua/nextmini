use std::collections::BTreeMap;
use std::sync::Arc;

use days::flows::packet::Packet;
use parking_lot::Mutex;

pub const MAILBOX_CAPACITY: usize = 256;

#[derive(Clone, Debug, Default)]
pub struct MailboxTracker {
    inner: Arc<Mutex<BTreeMap<&'static str, MailboxLevel>>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct MailboxLevel {
    current: usize,
    high_water: usize,
}

impl MailboxTracker {
    pub fn enqueue(&self, mailbox: &'static str) {
        let mut levels = self.inner.lock();
        let level = levels.entry(mailbox).or_default();
        level.current = level.current.saturating_add(1);
        level.high_water = level.high_water.max(level.current);
    }

    pub fn dequeue(&self, mailbox: &'static str) {
        let mut levels = self.inner.lock();
        let level = levels.entry(mailbox).or_default();
        level.current = level.current.saturating_sub(1);
    }

    pub fn high_water_marks(&self) -> BTreeMap<&'static str, usize> {
        self.inner
            .lock()
            .iter()
            .map(|(&name, level)| (name, level.high_water))
            .collect()
    }

    pub fn all_drained(&self) -> bool {
        self.inner.lock().values().all(|level| level.current == 0)
    }
}

#[derive(Clone, Debug)]
pub struct TrackedPacket {
    pub packet: Packet,
    target_mailbox: &'static str,
    tracker: MailboxTracker,
}

impl TrackedPacket {
    pub fn enqueue(packet: Packet, target_mailbox: &'static str, tracker: &MailboxTracker) -> Self {
        tracker.enqueue(target_mailbox);
        Self {
            packet,
            target_mailbox,
            tracker: tracker.clone(),
        }
    }

    pub fn arrive(self, expected_mailbox: &'static str) -> Packet {
        debug_assert_eq!(self.target_mailbox, expected_mailbox);
        self.tracker.dequeue(self.target_mailbox);
        self.packet
    }
}
