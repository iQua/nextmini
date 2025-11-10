use std::ffi::{c_char, c_int, c_void};

use bytes::Bytes;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyclass]
#[derive(Clone, Debug)]
pub struct FrozenBuffer {
    pub(crate) inner: Bytes,
}

#[pymethods]
impl FrozenBuffer {
    #[new]
    fn new(data: &Bound<'_, PyBytes>) -> Self {
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
                "FrozenBuffer is read-only",
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

            (*view).obj = slf.into_ptr();
        }

        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut pyo3::ffi::Py_buffer) {}
}

impl FrozenBuffer {
    /// Internal constructor from Bytes (not exposed to Python)
    pub fn from_bytes(bytes: Bytes) -> Self {
        Self { inner: bytes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_buffer_empty() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"");
            let buffer = FrozenBuffer::new(&data);
            assert_eq!(buffer.__len__(), 0);
        });
    }

    #[test]
    fn frozen_buffer_read_roundtrip() {
        Python::attach(|py| {
            let original = b"test data";
            let data = PyBytes::new(py, original);
            let buffer = FrozenBuffer::new(&data);
            let readback = buffer.read(py);
            assert_eq!(buffer.__len__(), 9);
            assert_eq!(readback.as_bytes(), original);
        });
    }
    #[test]
    fn frozen_buffer_slice_with_length() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer = FrozenBuffer::new(&data);
            let sliced = buffer.slice(2, Some(5)).expect("slice");
            assert_eq!(sliced.__len__(), 5);
            assert_eq!(sliced.read(py).as_bytes(), b"23456");
        });
    }

    #[test]
    fn frozen_buffer_slice_to_end() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer = FrozenBuffer::new(&data);
            let sliced = buffer.slice(5, None).expect("slice");
            assert_eq!(sliced.__len__(), 5);
            assert_eq!(sliced.read(py).as_bytes(), b"56789");
        });
    }

    #[test]
    fn frozen_buffer_slice_empty() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer = FrozenBuffer::new(&data);
            let sliced = buffer.slice(5, Some(0)).expect("slice");
            assert_eq!(sliced.__len__(), 0);
        });
    }

    #[test]
    fn frozen_buffer_slice_at_boundary() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"0123456789");
            let buffer = FrozenBuffer::new(&data);
            // Start at beginning
            let sliced = buffer.slice(0, Some(10)).expect("slice");
            assert_eq!(sliced.read(py).as_bytes(), b"0123456789");
            // Start at end
            let sliced_end = buffer.slice(10, None).expect("slice");
            assert_eq!(sliced_end.__len__(), 0);
        });
    }

    #[test]
    fn frozen_buffer_slice_start_exceeds_length() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"01234");
            let buffer = FrozenBuffer::new(&data);
            let err = buffer.slice(10, None).unwrap_err();
            assert!(err.to_string().contains("exceeds buffer length"));
        });
    }

    #[test]
    fn frozen_buffer_slice_end_exceeds_length() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"01234");
            let buffer = FrozenBuffer::new(&data);
            let err = buffer.slice(2, Some(10)).unwrap_err();
            assert!(err.to_string().contains("exceeds buffer length"));
        });
    }

    #[test]
    fn frozen_buffer_slice_length_overflow() {
        Python::attach(|py| {
            let data = PyBytes::new(py, b"01234");
            let buffer = FrozenBuffer::new(&data);
            // Try to slice starting at 2 with length that would overflow
            let err = buffer.slice(2, Some(usize::MAX)).unwrap_err();
            assert!(err.to_string().contains("overflow"));
        });
    }
}
