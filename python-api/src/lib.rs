use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use once_cell::sync::OnceCell;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::any::PyAnyMethods;
use pyo3::types::{PyAny, PyBytes, PyIterator};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinHandle;

use nextmini::node::conductor::Conductor;
use nextmini::node::config::LocalConfig;
use nextmini::node::packet::Packet;
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::python::interface::PythonInterfaceHandle;
use nextmini::node::{FlowId, NodeId, NodeIdExt};

const IP_HEADER_LEN: usize = 20;
const TCP_HEADER_LEN: usize = 20;

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("nextmini-py")
            .build()
            .expect("failed to create nextmini_py runtime")
    })
}

fn block_on<F>(future: F) -> F::Output
where
    F: std::future::Future,
{
    runtime().block_on(future)
}

fn payload_to_vec(py: Python<'_>, payload: &PyAny) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = payload.downcast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }

    let tobytes = payload.call_method0("tobytes")?;
    let bytes = tobytes.downcast::<PyBytes>()?;
    Ok(bytes.as_bytes().to_vec())
}

#[pyclass]
struct PacketReceiver {
    flow_id: FlowId,
    receiver: Arc<Mutex<mpsc::Receiver<Packet>>>,
}

impl PacketReceiver {
    fn new(flow_id: FlowId, receiver: mpsc::Receiver<Packet>) -> Self {
        Self {
            flow_id,
            receiver: Arc::new(Mutex::new(receiver)),
        }
    }
}

#[pymethods]
impl PacketReceiver {
    #[getter]
    fn flow_id(&self) -> u128 {
        self.flow_id
    }

    #[pyo3(signature = (timeout_ms=None))]
    fn recv(&self, py: Python<'_>, timeout_ms: Option<u64>) -> PyResult<Option<Py<PyBytes>>> {
        let receiver = self.receiver.clone();
        let future = async move {
            if let Some(ms) = timeout_ms {
                let duration = Duration::from_millis(ms);
                match tokio::time::timeout(duration, async {
                    let mut guard = receiver.lock().await;
                    guard.recv().await
                })
                .await
                {
                    Ok(packet) => packet,
                    Err(_) => None,
                }
            } else {
                let mut guard = receiver.lock().await;
                guard.recv().await
            }
        };

        let maybe_packet = Python::allow_threads(py, || block_on(future));
        Ok(maybe_packet.map(|packet| PyBytes::new_bound(py, packet.bytes()).unbind()))
    }

    fn recv_async(&self, py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
        let receiver = self.receiver.clone();
        future_into_py(py, async move {
            let packet = {
                let mut guard = receiver.lock().await;
                guard.recv().await
            };

            #[allow(deprecated)]
            Python::with_gil(|py| -> PyResult<Option<Py<PyBytes>>> {
                Ok(packet.map(|pkt| PyBytes::new_bound(py, pkt.bytes()).unbind()))
            })
        })
    }
}

#[pyclass]
struct Dataplane {
    config: LocalConfig,
    processor: ProcessorHandle,
    py_interface: PythonInterfaceHandle,
    conductor: Arc<Conductor>,
    join_handle: JoinHandle<()>,
}

#[pymethods]
impl Dataplane {
    #[new]
    fn new(py: Python<'_>, config_path: &str) -> PyResult<Self> {
        let raw = std::fs::read_to_string(config_path)
            .map_err(|err| PyRuntimeError::new_err(format!("failed to read config: {err}")))?;
        let config: LocalConfig = toml::from_str(&raw)
            .map_err(|err| PyRuntimeError::new_err(format!("failed to parse config: {err}")))?;

        let conductor = Python::allow_threads(py, || block_on(Conductor::new(config.clone())));
        let conductor = Arc::new(conductor);
        let processor = conductor.processor_handle();

        let py_interface = PythonInterfaceHandle::new(config.channel_capacity);
        processor.connect_python_interface(py_interface.clone());

        let join_handle = runtime().spawn({
            let conductor = Arc::clone(&conductor);
            async move {
                conductor.run().await;
            }
        });

        Ok(Self {
            config,
            processor,
            py_interface,
            conductor,
            join_handle,
        })
    }

    fn flow_id_from_nodes(
        &self,
        src_node_id: usize,
        dst_node_id: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> u128 {
        let (src_ip, src_port, dst_ip, dst_port) = self.flow_parts(
            src_node_id as NodeId,
            dst_node_id as NodeId,
            src_port,
            dst_port,
        );
        Packet::flow_id_from_parts(src_ip, src_port, dst_ip, dst_port)
    }

    #[pyo3(signature = (src_node_id, src_port=None, dst_port=None))]
    fn register_receiver_from_node(
        &self,
        py: Python<'_>,
        src_node_id: usize,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<Py<PacketReceiver>> {
        let (src_ip, src_port, dst_ip, dst_port) = self.flow_parts(
            src_node_id as NodeId,
            self.config.node_id as NodeId,
            src_port,
            dst_port,
        );
        let flow_id = Packet::flow_id_from_parts(src_ip, src_port, dst_ip, dst_port);
        self.register_receiver_for_flow(py, flow_id)
    }

    fn register_receiver_for_flow(
        &self,
        py: Python<'_>,
        flow_id: u128,
    ) -> PyResult<Py<PacketReceiver>> {
        let future = self.py_interface.register_receiver(flow_id);
        let receiver = Python::allow_threads(py, || block_on(future));
        Py::new(py, PacketReceiver::new(flow_id, receiver))
    }

    #[pyo3(signature = (dst_node_id, payload, src_port=None, dst_port=None))]
    fn send_to_node(
        &self,
        py: Python<'_>,
        dst_node_id: usize,
        payload: &PyAny,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<()> {
        let (src_ip, src_port, dst_ip, dst_port) = self.flow_parts(
            self.config.node_id as NodeId,
            dst_node_id as NodeId,
            src_port,
            dst_port,
        );

        let payload_vec = payload_to_vec(py, payload)?;
        let packet =
            Packet::build_ipv4_tcp_packet(src_ip, src_port, dst_ip, dst_port, &payload_vec);
        Python::allow_threads(py, || self.processor.process_packet(packet));
        Ok(())
    }

    #[pyo3(signature = (dst_node_id, payloads, src_port=None, dst_port=None))]
    fn send_batch_to_node(
        &self,
        py: Python<'_>,
        dst_node_id: usize,
        payloads: &PyAny,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<()> {
        let iter = PyIterator::from_object(payloads)?;
        for item in iter {
            self.send_to_node(py, dst_node_id, item?, src_port, dst_port)?;
        }
        Ok(())
    }

    fn shutdown(&self) {
        self.join_handle.abort();
    }

    fn __del__(&mut self) {
        self.join_handle.abort();
    }
}

impl Dataplane {
    fn flow_parts(
        &self,
        src_node_id: NodeId,
        dst_node_id: NodeId,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> (Ipv4Addr, u16, Ipv4Addr, u16) {
        let src_ip =
            src_node_id.ip_addr(self.config.user_space_base_addr, self.config.local_netmask);
        let dst_ip =
            dst_node_id.ip_addr(self.config.user_space_base_addr, self.config.local_netmask);
        let src_port = src_port.unwrap_or(self.config.user_space_client_port);
        let dst_port = dst_port.unwrap_or(self.config.user_space_server_port);
        (src_ip, src_port, dst_ip, dst_port)
    }
}

#[pymodule]
fn nextmini_py(_py: Python<'_>, module: &PyModule) -> PyResult<()> {
    module.add_class::<Dataplane>()?;
    module.add_class::<PacketReceiver>()?;
    Ok(())
}
