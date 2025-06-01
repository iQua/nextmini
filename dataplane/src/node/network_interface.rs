use crate::node::packet::Packet;
use tokio::sync::mpsc;

// used in scheduler to send packet to network interface
// network interface need processor handle to send packet to processor
// created in controller interface
pub struct NetworkInterfaceHandle {
    sender: mpsc::Sender<Packet>,
}

impl NetworkInterfaceHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(100);
        Self { sender }
    }

    pub async fn send(&self, packet: Packet){
        
    }
}


