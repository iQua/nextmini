mod buffer;

use std::sync::Arc;

use once_cell::sync::OnceCell;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::PyModuleMethods;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyModule};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use nextmini::node::conductor::Conductor;
use nextmini::node::config::LocalConfig;
use nextmini::node::controller::interface::ControllerInterfaceHandle;
use nextmini::node::packet::Packet;
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::python::interface::{PythonEvent, PythonInterfaceHandle};
use nextmini::node::{GroupId, GroupIdExt, NodeId, NodeIdExt};
use nextmini_messages::DataplaneToController;

pub use crate::buffer::FrozenBuffer;

static RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();

fn rt() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("nextmini-py")
            .build()
            .expect("unable to create tokio runtime for nextmini_py")
    })
}

#[pyclass]
struct PacketReceiver {
    inner: Arc<Mutex<mpsc::Receiver<nextmini::node::packet::Packet>>>,
}

#[pymethods]
impl PacketReceiver {
    #[pyo3(signature = (timeout_ms=None))]
    fn recv(&self, timeout_ms: Option<u64>, py: Python<'_>) -> PyResult<Option<Py<PyBytes>>> {
        let inner = self.inner.clone();
        let fut = async move {
            match timeout_ms {
                Some(ms) => tokio::time::timeout(
                    std::time::Duration::from_millis(ms),
                    inner.lock().await.recv(),
                )
                .await
                .unwrap_or_default(),
                None => inner.lock().await.recv().await,
            }
        };
        let maybe_pkt = rt().block_on(fut);
        Ok(maybe_pkt.map(|p| PyBytes::new(py, p.bytes()).unbind()))
    }

    fn recv_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = self.inner.clone();
        future_into_py(py, async move {
            let mut guard = inner.lock().await;
            let pkt = guard.recv().await;
            drop(guard);
            Ok(pkt.map(|p| p.bytes().to_vec()))
        })
    }
}

#[pyclass]
struct Dataplane {
    cfg: LocalConfig,
    py_if: PythonInterfaceHandle,
    processor: ProcessorHandle,
    controller: ControllerInterfaceHandle,
    event_receiver: Arc<Mutex<mpsc::Receiver<PythonEvent>>>,
    _join: tokio::task::JoinHandle<()>,
}

#[pymethods]
impl Dataplane {
    #[new]
    fn new(config_path: &str) -> PyResult<Self> {
        let toml_str = std::fs::read_to_string(config_path)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to read config: {e}")))?;
        let mut initial_cfg = LocalConfig::from_toml_str(&toml_str)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to parse config: {e}")))?;
        initial_cfg.enable_local_interface = false;
        initial_cfg.config_path = config_path.to_string();
        initial_cfg.populate_runtime_defaults();

        // Create PythonInterface and register event receiver
        let py_if = PythonInterfaceHandle::new(initial_cfg.channel_capacity);
        let (event_rx, event_tx) = py_if.register_event_receiver();

        // Create conductor with event sender
        let conductor = rt().block_on(async {
            Conductor::new(initial_cfg.clone(), Some(event_tx)).await
        });

        let processor = conductor.processor_handle();
        let controller = conductor.controller_handle();
        let mut cfg = conductor.local_config();
        cfg.config_path = config_path.to_string();

        processor.connect_python_interface(py_if.clone());

        let join = rt().spawn(async move {
            conductor.run().await;
        });

        Ok(Self {
            cfg,
            py_if,
            processor,
            controller,
            event_receiver: Arc::new(Mutex::new(event_rx)),
            _join: join,
        })
    }

    #[pyo3(signature = (src_node_id, dst_node_id, src_port=None, dst_port=None))]
    fn flow_id_from_nodes(
        &self,
        src_node_id: usize,
        dst_node_id: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<u128> {
        let src_ip =
            (src_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let dst_ip =
            (dst_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
        Ok(Packet::flow_id_from_parts(src_ip, sp, dst_ip, dp))
    }

    #[pyo3(signature = (src_node_id, src_port=None, dst_port=None))]
    fn register_receiver_from_node(
        &self,
        py: Python<'_>,
        src_node_id: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<Py<PacketReceiver>> {
        let src_ip =
            (src_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let dst_ip = self.cfg.user_space_address;
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
        let flow_id = Packet::flow_id_from_parts(src_ip, sp, dst_ip, dp);

        let rx = rt().block_on(self.py_if.register_receiver(flow_id));
        Py::new(
            py,
            PacketReceiver {
                inner: Arc::new(Mutex::new(rx)),
            },
        )
    }

    #[pyo3(signature = (dst_node_id, frozen, src_port=None, dst_port=None))]
    fn send_to_node(
        &self,
        dst_node_id: usize,
        frozen: FrozenBuffer,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<()> {
        // Clone Bytes (zero-copy reference counting)
        let body = frozen.inner.clone();

        let src_ip = self.cfg.user_space_address;
        let dst_ip =
            (dst_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);

        let packet = Packet::build_ipv4_tcp_packet(src_ip, sp, dst_ip, dp, &body);
        self.processor.process_packet(packet);
        Ok(())
    }

    #[pyo3(signature = (dst_node_id, frozen_buffers, src_port=None, dst_port=None))]
    fn send_batch_to_node(
        &self,
        dst_node_id: usize,
        frozen_buffers: Vec<FrozenBuffer>,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<()> {
        for frozen in frozen_buffers {
            self.send_to_node(dst_node_id, frozen, src_port, dst_port)?;
        }
        Ok(())
    }

    /// Creates a multicast group and waits for broadcast confirmation.
    /// Returns True if creation succeeded, False if timeout or creation failed.
    /// If label is not provided, it defaults to "group-{group_id}".
    #[pyo3(signature = (group_id, label=None, timeout_ms=10000))]
    fn create_group_wait(
        &self,
        group_id: usize,
        label: Option<&str>,
        timeout_ms: u64,
    ) -> PyResult<bool> {
        let my_node_id = self.cfg.node_id;
        let event_receiver = self.event_receiver.clone();

        // auto-generates label if not provided
        let label = label
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("group-{}", group_id));

        // sends create group message
        let msg = DataplaneToController::CreateGroup {
            group_id,
            label: label.clone(),
        };
        rt().block_on(self.controller.send(msg));

        // waits for confirmation from the receiver
        let timeout = std::time::Duration::from_millis(timeout_ms);
        match rt().block_on(async {
            tokio::time::timeout(timeout, async {
                let mut rx = event_receiver.lock().await;
                loop {
                    match rx.recv().await {
                        Some(PythonEvent::GroupCreated {
                            group_id: gid,
                            src_node_id,
                            success,
                            error,
                            ..
                        }) if gid == group_id && src_node_id == my_node_id => {
                            if !success {
                                if let Some(err) = error {
                                    eprintln!("Group creation failed: {}", err);
                                }
                            }
                            return success;
                        }
                        None => return false,
                        _ => continue,
                    }
                }
            })
            .await
        }) {
            Ok(success) => Ok(success),
            Err(_) => Err(PyRuntimeError::new_err(format!(
                "Timeout waiting for group {} creation confirmation after {}ms.",
                group_id, timeout_ms
            ))),
        }
    }

    /// Waits for a multicast group to be created by another node.
    /// Returns True if group was created, False if timeout.
    #[pyo3(signature = (group_id, timeout_ms=30000))]
    fn wait_for_group_created(&self, group_id: usize, timeout_ms: u64) -> PyResult<bool> {
        let event_receiver = self.event_receiver.clone();

        // waits for group creation from the pre-created receiver (from any node)
        let timeout = std::time::Duration::from_millis(timeout_ms);
        match rt().block_on(async {
            tokio::time::timeout(timeout, async {
                let mut rx = event_receiver.lock().await;
                loop {
                    match rx.recv().await {
                        Some(PythonEvent::GroupCreated {
                            group_id: gid,
                            success,
                            ..
                        }) if gid == group_id && success => {
                            return true;
                        }
                        None => return false,  // Channel closed
                        _ => continue,  // Different event, keep waiting
                    }
                }
            })
            .await
        }) {
            Ok(created) => Ok(created),
            Err(_) => Err(PyRuntimeError::new_err(format!(
                "Timeout waiting for group {} creation after {}ms.",
                group_id, timeout_ms
            ))),
        }
    }

    /// Joins an existing multicast group by group_id.
    fn join_group(&self, group_id: usize) -> PyResult<()> {
        let msg = DataplaneToController::JoinGroup { group_id };
        rt().block_on(self.controller.send(msg));
        Ok(())
    }

    /// Leaves a multicast group by group_id.
    fn leave_group(&self, group_id: usize) -> PyResult<()> {
        let msg = DataplaneToController::LeaveGroup { group_id };
        rt().block_on(self.controller.send(msg));
        Ok(())
    }

    /// Sends data to a multicast group using group_id.
    /// The group_ip is calculated automatically: multicast_pool_base + group_id.
    #[pyo3(signature = (group_id, frozen, src_port=None, dst_port=None))]
    fn send_to_group(
        &self,
        group_id: usize,
        frozen: FrozenBuffer,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<()> {
        // Calculate group_ip from group_id (deterministic)
        let group_ip = (group_id as GroupId).group_ip(self.cfg.multicast_pool_base);

        let body = frozen.inner.clone();
        let src_ip = self.cfg.user_space_address;
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);

        let packet = Packet::build_ipv4_tcp_packet(src_ip, sp, group_ip, dp, &body);
        self.processor.process_packet(packet);
        Ok(())
    }

    /// Registers a receiver for multicast packets from a specific source to a group.
    /// Returns a PacketReceiver that can be used to receive packets.
    /// The group_ip is calculated automatically from group_id.
    #[pyo3(signature = (group_id, src_node_id, src_port=None, dst_port=None))]
    fn register_group_receiver(
        &self,
        py: Python<'_>,
        group_id: usize,
        src_node_id: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<Py<PacketReceiver>> {
        // Calculate group_ip from group_id (deterministic)
        let group_ip = (group_id as GroupId).group_ip(self.cfg.multicast_pool_base);

        let src_ip =
            (src_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);
        let flow_id = Packet::flow_id_from_parts(src_ip, sp, group_ip, dp);

        let rx = rt().block_on(self.py_if.register_receiver(flow_id));
        Py::new(
            py,
            PacketReceiver {
                inner: Arc::new(Mutex::new(rx)),
            },
        )
    }
}

#[pymodule]
fn nextmini_py(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Dataplane>()?;
    m.add_class::<PacketReceiver>()?;
    m.add_class::<FrozenBuffer>()?;
    Ok(())
}
