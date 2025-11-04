use std::sync::Arc;

use once_cell::sync::OnceCell;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::prelude::PyModuleMethods;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyIterator, PyModule};
use pyo3_async_runtimes::tokio::future_into_py;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use nextmini::node::conductor::Conductor;
use nextmini::node::config::LocalConfig;
use nextmini::node::packet::Packet;
use nextmini::node::processor::ProcessorHandle;
use nextmini::node::python::interface::PythonInterfaceHandle;
use nextmini::node::{NodeId, NodeIdExt};

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

fn payload_to_vec(payload: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = payload.cast::<PyBytes>() {
        return Ok(bytes.as_bytes().to_vec());
    }

    let tobytes = payload.call_method0("tobytes").map_err(|_| {
        PyRuntimeError::new_err("payload must expose a contiguous buffer via `tobytes()`")
    })?;
    let bytes = tobytes
        .cast::<PyBytes>()
        .map_err(|_| PyRuntimeError::new_err("`tobytes()` must return `bytes`"))?;
    Ok(bytes.as_bytes().to_vec())
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
                Some(ms) => match tokio::time::timeout(
                    std::time::Duration::from_millis(ms),
                    inner.lock().await.recv(),
                )
                .await
                {
                    Ok(opt) => opt,
                    Err(_) => None,
                },
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
    _join: tokio::task::JoinHandle<()>,
}

#[pymethods]
impl Dataplane {
    #[new]
    fn new(config_path: &str) -> PyResult<Self> {
        let toml_str = std::fs::read_to_string(config_path)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to read config: {e}")))?;
        let cfg: LocalConfig = toml::from_str(&toml_str)
            .map_err(|e| PyRuntimeError::new_err(format!("failed to parse config: {e}")))?;

        let conductor = rt().block_on(async { Conductor::new(cfg.clone()).await });
        let processor = conductor.processor_handle();

        let py_if = PythonInterfaceHandle::new(cfg.channel_capacity);
        processor.connect_python_interface(py_if.clone());

        let join = rt().spawn(async move {
            conductor.run().await;
        });

        Ok(Self {
            cfg,
            py_if,
            processor,
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

    #[pyo3(signature = (dst_node_id, payload, src_port=None, dst_port=None))]
    fn send_to_node(
        &self,
        dst_node_id: usize,
        payload: &Bound<'_, PyAny>,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<()> {
        let body = payload_to_vec(payload)?;

        let src_ip = self.cfg.user_space_address;
        let dst_ip =
            (dst_node_id as NodeId).ip_addr(self.cfg.user_space_base_addr, self.cfg.local_netmask);
        let sp = src_port.unwrap_or(self.cfg.user_space_client_port);
        let dp = dst_port.unwrap_or(self.cfg.user_space_server_port);

        let packet = Packet::build_ipv4_tcp_packet(src_ip, sp, dst_ip, dp, &body);
        self.processor.process_packet(packet);
        Ok(())
    }

    #[pyo3(signature = (dst_node_id, payloads, src_port=None, dst_port=None))]
    fn send_batch_to_node(
        &self,
        dst_node_id: usize,
        payloads: &Bound<'_, PyAny>,
        src_port: Option<u16>,
        dst_port: Option<u16>,
    ) -> PyResult<()> {
        let iter = PyIterator::from_object(payloads)?;
        for item in iter {
            let obj = item?;
            self.send_to_node(dst_node_id, &obj, src_port, dst_port)?;
        }
        Ok(())
    }
}

#[pymodule]
fn nextmini_py(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Dataplane>()?;
    m.add_class::<PacketReceiver>()?;
    Ok(())
}
