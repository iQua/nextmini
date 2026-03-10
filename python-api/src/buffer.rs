use std::ffi::{c_char, c_int, c_void};

use bytes::{Bytes, BytesMut};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// Mutable builder for constructing packets incrementally.
#[pyclass]
#[derive(Debug)]
pub struct PacketBuilder {
    inner: BytesMut,
}

#[pymethods]
impl PacketBuilder {
    #[new]
    #[pyo3(signature = (size=4096))]
    fn new(size: usize) -> Self {
        Self {
            inner: BytesMut::with_capacity(size),
        }
    }

    fn write(&mut self, data: &Bound<'_, PyBytes>) -> usize {
        let bytes = data.as_bytes();
        self.inner.extend_from_slice(bytes);
        bytes.len()
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    /// Zero-copy conversion to immutable PacketView.
    fn freeze(&mut self) -> PacketView {
        let bytes = std::mem::take(&mut self.inner).freeze();
        PacketView { inner: bytes }
    }
}

/// Read-only, reference-counted view of packet data.
#[pyclass(from_py_object)]
#[derive(Clone, Debug)]
pub struct PacketView {
    pub(crate) inner: Bytes,
}

#[pymethods]
impl PacketView {
    #[new]
    fn new(data: &Bound<'_, PyBytes>) -> Self {
        Self {
            inner: Bytes::copy_from_slice(data.as_bytes()),
        }
    }

    #[staticmethod]
    fn from_buffer(data: &Bound<'_, PyBytes>) -> Self {
        Self {
            inner: Bytes::copy_from_slice(data.as_bytes()),
        }
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn read<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner)
    }

    #[pyo3(signature = (start, length=None))]
    fn slice(&self, start: usize, length: Option<usize>) -> PyResult<Self> {
        let total = self.inner.len();
        if start > total {
            return Err(PyValueError::new_err(format!(
                "slice start {} exceeds buffer length {}",
                start, total
            )));
        }

        let end = match length {
            Some(len) => start
                .checked_add(len)
                .ok_or_else(|| PyValueError::new_err("slice length overflow"))?,
            None => total,
        };

        if end > total {
            return Err(PyValueError::new_err(format!(
                "slice end {} exceeds buffer length {}",
                end, total
            )));
        }

        Ok(Self {
            inner: self.inner.slice(start..end),
        })
    }

    unsafe fn __getbuffer__(
        slf: PyRefMut<'_, Self>,
        view: *mut pyo3::ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        if view.is_null() {
            return Err(pyo3::exceptions::PyBufferError::new_err(
                "view pointer is null",
            ));
        }

        if (flags & pyo3::ffi::PyBUF_WRITABLE) == pyo3::ffi::PyBUF_WRITABLE {
            return Err(pyo3::exceptions::PyBufferError::new_err(
                "PacketView is read-only",
            ));
        }

        let bytes = &slf.inner;

        // Format string for unsigned char buffer protocol
        static FORMAT: &[u8] = b"B\0";

        unsafe {
            (*view).buf = bytes.as_ptr() as *mut c_void;
            (*view).len = bytes.len() as isize;
            (*view).readonly = 1;
            (*view).itemsize = 1;
            // Format string: "B" = unsigned char (required for proper buffer protocol support)
            (*view).format = FORMAT.as_ptr() as *mut c_char;
            (*view).ndim = 1;
            // For 1D contiguous arrays, shape and strides can be NULL (means C-contiguous)
            // Previously these pointed to stack memory which would be invalid after return
            (*view).shape = std::ptr::null_mut();
            (*view).strides = std::ptr::null_mut();
            (*view).suboffsets = std::ptr::null_mut();
            (*view).internal = std::ptr::null_mut();

            let obj_ptr = slf.into_ptr();
            pyo3::ffi::Py_INCREF(obj_ptr);
            (*view).obj = obj_ptr;
        }

        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut pyo3::ffi::Py_buffer) {}
}

impl PacketView {
    /// Internal constructor from Bytes (not exposed to Python)
    pub fn from_bytes(bytes: Bytes) -> Self {
        Self { inner: bytes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_view_empty() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"");
            let buffer = PacketView::new(&data);
            assert_eq!(buffer.__len__(), 0);
        });
    }

    #[test]
    fn packet_view_read_roundtrip() {
        Python::attach(|py| {
            let original = b"test data";
            let data = PyBytes::new(py, original);
            let buffer = PacketView::new(&data);
            let readback = buffer.read(py);
            assert_eq!(buffer.__len__(), 9);
            assert_eq!(readback.as_bytes(), original);
        });
    }
    #[test]
    fn packet_view_slice_with_length() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer = PacketView::new(&data);
            let sliced = buffer.slice(2, Some(5)).expect("slice");
            assert_eq!(sliced.__len__(), 5);
            assert_eq!(sliced.read(py).as_bytes(), b"23456");
        });
    }

    #[test]
    fn packet_view_slice_to_end() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer = PacketView::new(&data);
            let sliced = buffer.slice(5, None).expect("slice");
            assert_eq!(sliced.__len__(), 5);
            assert_eq!(sliced.read(py).as_bytes(), b"56789");
        });
    }

    #[test]
    fn packet_view_slice_empty() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer = PacketView::new(&data);
            let sliced = buffer.slice(5, Some(0)).expect("slice");
            assert_eq!(sliced.__len__(), 0);
        });
    }

    #[test]
    fn packet_view_slice_at_boundary() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer = PacketView::new(&data);
            // Start at beginning
            let sliced = buffer.slice(0, Some(10)).expect("slice");
            assert_eq!(sliced.read(py).as_bytes(), b"0123456789");
            // Start at end
            let sliced_end = buffer.slice(10, None).expect("slice");
            assert_eq!(sliced_end.__len__(), 0);
        });
    }

    #[test]
    fn packet_view_slice_start_exceeds_length() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"01234");
            let buffer = PacketView::new(&data);
            let err = buffer.slice(10, None).unwrap_err();
            assert!(err.to_string().contains("exceeds buffer length"));
        });
    }

    #[test]
    fn packet_view_slice_end_exceeds_length() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"01234");
            let buffer = PacketView::new(&data);
            let err = buffer.slice(2, Some(10)).unwrap_err();
            assert!(err.to_string().contains("exceeds buffer length"));
        });
    }

    #[test]
    fn packet_view_slice_length_overflow() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"01234");
            let buffer = PacketView::new(&data);
            // Try to slice starting at 2 with length that would overflow
            let err = buffer.slice(2, Some(usize::MAX)).unwrap_err();
            assert!(err.to_string().contains("overflow"));
        });
    }

    #[test]
    fn packet_view_from_bytes() {
        let bytes = Bytes::from_static(b"internal");
        let buffer = PacketView::from_bytes(bytes);
        assert_eq!(buffer.__len__(), 8);
        assert_eq!(&buffer.inner[..], b"internal");
    }

    #[test]
    fn packet_view_clone() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer1 = PacketView::new(&data);
            let buffer2 = buffer1.clone();
            assert_eq!(buffer1.__len__(), buffer2.__len__());
            assert_eq!(buffer1.read(py).as_bytes(), buffer2.read(py).as_bytes());
        });
    }

    #[test]
    fn packet_view_clone_shares_memory() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer1 = PacketView::new(&data);
            let buffer2 = buffer1.clone();

            // Both should point to the same memory address
            assert_eq!(
                buffer1.inner.as_ptr(),
                buffer2.inner.as_ptr(),
                "Clones should share the same underlying memory"
            );
        });
    }

    #[test]
    fn packet_builder_empty() {
        let builder = PacketBuilder::new(100);
        assert_eq!(builder.__len__(), 0);
    }

    #[test]
    fn packet_builder_write_and_freeze() {
        Python::attach(|py| {
            let mut builder = PacketBuilder::new(100);
            let chunk1 = PyBytes::new(py, b"hello");
            let chunk2 = PyBytes::new(py, b"world");

            assert_eq!(builder.write(&chunk1), 5);
            assert_eq!(builder.write(&chunk2), 5);
            assert_eq!(builder.__len__(), 10);

            let view = builder.freeze();
            assert_eq!(view.__len__(), 10);
            assert_eq!(view.read(py).as_bytes(), b"helloworld");

            // Builder should be empty after freeze
            assert_eq!(builder.__len__(), 0);
        });
    }

    #[test]
    fn packet_builder_incremental_large() {
        Python::attach(|py| {
            let mut builder = PacketBuilder::new(1024);

            for i in 0..100 {
                let data = format!("chunk{:03}", i);
                let bytes = PyBytes::new(py, data.as_bytes());
                builder.write(&bytes);
            }

            assert_eq!(builder.__len__(), 800);

            let view = builder.freeze();
            assert_eq!(view.__len__(), 800);

            let content = view.read(py);
            assert!(content.as_bytes().starts_with(b"chunk000"));
            assert!(content.as_bytes().ends_with(b"chunk099"));
        });
    }

    #[test]
    fn packet_builder_zero_copy_freeze() {
        Python::attach(|py| {
            let mut builder = PacketBuilder::new(100);
            let data = PyBytes::new(py, b"test data for zero copy");
            builder.write(&data);

            let ptr_before = builder.inner.as_ptr();
            let view = builder.freeze();
            let ptr_after = view.inner.as_ptr();

            assert_eq!(ptr_before, ptr_after, "freeze() should be zero-copy");
        });
    }
}
