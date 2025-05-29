use crate::node::config::LocalConfig;
use crate::node::processor::ProcessorHandleforController;
use nextmini_messages::{ControllerToDataplane, DataplaneToController};

use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio::sync::mpsc;
use tokio::select;
use tracing::{info,error};


enum CoordinatorMessage {
    SendToController(DataplaneToController),
    Shutdown,
}


#[derive(Clone)]
pub struct CoordinatorHandle {
    sender: mpsc::Sender<CoordinatorMessage>,
}

impl CoordinatorHandle {
    pub fn new(config: LocalConfig, shutdown_sender: mpsc::UnboundedReceiver<()>) -> Self {
        // Create coordinator handle mpsc channel
        // Question : Creat ws connection here or in conductor? Connect to actual controller with ws
        // Create and spawn coordinator
        // return self
    }

    pub async fn shutdown_coordinator(&self) {
        self.sender
            .send(CoordinatorMessage::Shutdown)
            .await
            .expect("Failed to shutdown the coordinator.");
    }

    pub async fn send_to_controller(&self, data: DataplaneToController){
        self.sender
        .send(CoordinatorMessage::SendToController(data))
        .await
        .expect("Fail to send DataplaneToController message to coordinator");
    }
}

pub struct Coordinator {
    // Receiving from coordinator handle
    receiver: mpsc::Receiver<CoordinatorMessage>, 

    // For connection with the actual coordinator 
    controller_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    sender_tx: UnboundedSender<DataplaneToController>,
    sender_rx: UnboundedReceiver<DataplaneToController>,
    
    // Processor Handles
    processor_handle: ProcessorHandleforController,
}

impl Coordinator {
    pub async fn run(&mut self) {
        loop {
            select!{
                Some(controller_handle_msg) = self.receiver.recv() => {
                    if let CoordinatorMessage::SendToController(msg) = controller_handle_msg{
                        self.sender_tx.send(msg);
                    }else{
                        info!("Shutting down coordinator interface anyway.");
                        break;
                    }
                }
                // TODO : Receive message from controller

            };
        }
    }

    async fn broadcast_msg_to_processors(&mut self, msg: ControllerToDataplane) {
        match msg {
            ControllerToDataplane::AddNode {
                protocol,
                node_id,
                addr,
            } => {
                // Use network interface
                // 1. Request remote node connection via protocol protocols_client
                // 2. get protocol writer handle
                // 3. Create scheduler handle
                // 4. Send add node message via processor handle
                return;
            }
            ControllerToDataplane::SetLinkRate { node_id, rate } => {
                // TODO : Implement set link rate
                return;
            }
            ControllerToDataplane::InstallRoutes { routes } => {
                info!("Installing {} routes.", routes.len());
                self.processor_handle.update_routing_table(routes).await;
            }
            _ => error!("Received unsupported message type."),
        }
    }
}