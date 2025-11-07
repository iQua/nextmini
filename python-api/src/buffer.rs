use std::ffi::{c_char, c_int, c_void};

use bytes::Bytes;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

#[pyclass]
#[derive(Clone)]
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
