use std::collections::VecDeque;

use days::flows::packet::Packet;
use days::flows::tcp_socket::{TcpSocketError, TcpSocketReceiver, TcpSocketSender};

use crate::protocol::ControlFrame;
use crate::transport::{SocketPairConfig, seconds_from_ns};

use super::{ControlStream, ControlStreamCursor};

#[derive(Debug)]
pub(crate) struct ControlTx {
    flow_id: usize,
    sender: TcpSocketSender,
    stream: ControlStream,
    pending: VecDeque<ControlFrame>,
}

pub(crate) struct SubmittedControl {
    pub(crate) frame: ControlFrame,
    pub(crate) wire_bytes: usize,
}

impl ControlTx {
    pub(crate) fn new(
        flow_id: usize,
        socket: SocketPairConfig,
        stream: ControlStream,
    ) -> Result<Self, TcpSocketError> {
        Ok(Self {
            flow_id,
            sender: socket.sender(flow_id, 0)?,
            stream,
            pending: VecDeque::new(),
        })
    }

    pub(crate) fn flow_id(&self) -> usize {
        self.flow_id
    }

    pub(crate) fn queue(&mut self, frame: ControlFrame) {
        self.pending.push_back(frame);
    }

    pub(crate) fn drive(
        &mut self,
        now_ns: u64,
    ) -> Result<(Vec<Packet>, Vec<SubmittedControl>), TcpSocketError> {
        let mut submitted = Vec::new();
        while let Some(frame) = self.pending.front() {
            let wire_bytes = frame
                .payload_bytes()
                .checked_add(4)
                .ok_or(TcpSocketError::SequenceOverflow)?;
            if self.sender.writable_bytes() < wire_bytes {
                break;
            }
            let frame = self.pending.pop_front().expect("front was present");
            let appended = self
                .stream
                .append(frame.clone())
                .map_err(|_| TcpSocketError::SequenceOverflow)?;
            debug_assert_eq!(appended, wire_bytes);
            let admission = self.sender.admit_application_write(wire_bytes)?;
            debug_assert_eq!(admission.accepted_bytes, wire_bytes);
            debug_assert_eq!(admission.blocked_bytes, 0);
            submitted.push(SubmittedControl { frame, wire_bytes });
        }
        let packets = self.sender.poll_transmit(seconds_from_ns(now_ns))?;
        Ok((packets, submitted))
    }

    pub(crate) fn receive_ack(
        &mut self,
        packet: &Packet,
        now_ns: u64,
    ) -> Result<Vec<Packet>, TcpSocketError> {
        self.sender.receive_ack(packet, seconds_from_ns(now_ns))
    }

    pub(crate) fn timer(&mut self, now_ns: u64) -> Result<Vec<Packet>, TcpSocketError> {
        self.sender.timer_tick(seconds_from_ns(now_ns))
    }
}

#[derive(Debug)]
pub(crate) struct ControlRx {
    flow_id: usize,
    receiver: TcpSocketReceiver,
    stream: ControlStream,
    cursor: ControlStreamCursor,
    read_credit_chunk: usize,
}

impl ControlRx {
    pub(crate) fn new(
        flow_id: usize,
        socket: SocketPairConfig,
        stream: ControlStream,
    ) -> Result<Self, TcpSocketError> {
        Ok(Self {
            flow_id,
            receiver: TcpSocketReceiver::new(flow_id, 0, socket.socket)?,
            stream,
            cursor: ControlStreamCursor::default(),
            read_credit_chunk: socket.socket.receive_buffer_bytes,
        })
    }

    pub(crate) fn flow_id(&self) -> usize {
        self.flow_id
    }

    pub(crate) fn start(&mut self, now_ns: u64) -> Result<Vec<Packet>, TcpSocketError> {
        let outcome = self
            .receiver
            .grant_read_credit(self.read_credit_chunk, seconds_from_ns(now_ns))?;
        Ok(vec![outcome.acknowledgment])
    }

    pub(crate) fn receive(
        &mut self,
        packet: &Packet,
        now_ns: u64,
    ) -> Result<(Vec<Packet>, Vec<ControlFrame>), TcpSocketError> {
        let mut acknowledgments = Vec::new();
        let mut controls = Vec::new();
        let mut outcome = self
            .receiver
            .receive_segment(packet, seconds_from_ns(now_ns))?;
        acknowledgments.push(outcome.acknowledgment);
        while let Some(delivered) = outcome.delivered {
            controls.extend(
                self.cursor
                    .ingest(&self.stream, delivered.stream_offset, delivered.byte_count)
                    .map_err(|_| TcpSocketError::SequenceOverflow)?,
            );
            outcome = self
                .receiver
                .grant_read_credit(delivered.byte_count, seconds_from_ns(now_ns))?;
            acknowledgments.push(outcome.acknowledgment);
        }
        Ok((acknowledgments, controls))
    }
}
