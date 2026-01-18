use std::ffi::{c_char, c_int, c_void};
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::{mem, slice};

use bytes::{Bytes, BytesMut};
use pyo3::PyErr;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes};

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
#[pyclass]
#[derive(Clone, Debug)]
pub struct PacketView {
    pub(crate) inner: Bytes,
}

#[repr(transparent)]
struct RawPyBuffer(pyo3::ffi::Py_buffer, PhantomPinned);

/// Owns a Python `bytes` object while borrowing its immutable backing store.
struct OwnedPyBytes {
    bytes: Py<PyBytes>,
}

impl OwnedPyBytes {
    fn new(bytes: &Bound<'_, PyBytes>) -> Self {
        Self {
            bytes: bytes.clone().unbind(),
        }
    }
}

impl AsRef<[u8]> for OwnedPyBytes {
    fn as_ref(&self) -> &[u8] {
        let (ptr, len) = Python::attach(|py| {
            let bytes = self.bytes.bind(py);
            let slice = bytes.as_bytes();
            (slice.as_ptr(), slice.len())
        });

        if len == 0 {
            return &[];
        }

        // Safety: `self.bytes` keeps the underlying `bytes` alive, and its
        // contents are immutable.
        unsafe { slice::from_raw_parts(ptr, len) }
    }
}

/// Owning view over a Python buffer-protocol exporter.
///
/// `Py_buffer` exporters may create self-referential views; we pin the view in
/// memory to keep any internal pointers valid for the lifetime of this struct.
struct OwnedPyBuffer {
    view: Pin<Box<RawPyBuffer>>,
    len: usize,
}

impl OwnedPyBuffer {
    fn get(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        let py = obj.py();
        let mut view = Box::new(mem::MaybeUninit::<RawPyBuffer>::uninit());
        let rc = unsafe {
            pyo3::ffi::PyObject_GetBuffer(
                obj.as_ptr(),
                view.as_mut_ptr().cast::<pyo3::ffi::Py_buffer>(),
                pyo3::ffi::PyBUF_SIMPLE,
            )
        };

        if rc != 0 {
            return Err(PyErr::fetch(py));
        }

        // Safety: `PyObject_GetBuffer` initialized the `Py_buffer` portion of
        // `RawPyBuffer`. `PhantomPinned` is a ZST.
        // TODO: replace with Box::assume_init once MSRV allows it.
        let view: Box<RawPyBuffer> = unsafe { mem::transmute(view) };
        let mut view = Pin::from(view);

        struct BufferReleaseGuard {
            view: *mut pyo3::ffi::Py_buffer,
            active: bool,
        }

        impl BufferReleaseGuard {
            unsafe fn new(view: &mut Pin<Box<RawPyBuffer>>) -> Self {
                Self {
                    view: &mut unsafe { Pin::get_unchecked_mut(view.as_mut()) }.0,
                    active: true,
                }
            }

            fn disarm(&mut self) {
                self.active = false;
            }
        }

        impl Drop for BufferReleaseGuard {
            fn drop(&mut self) {
                if !self.active {
                    return;
                }

                // Safety: the view was initialized by `PyObject_GetBuffer`.
                unsafe {
                    pyo3::ffi::PyBuffer_Release(self.view);
                }
            }
        }

        let mut guard = unsafe { BufferReleaseGuard::new(&mut view) };
        let raw = unsafe { &*guard.view };
        let raw_len = raw.len;
        let raw_buf = raw.buf;

        let len = usize::try_from(raw_len).map_err(|_| {
            PyValueError::new_err(format!("buffer length {} does not fit into usize", raw_len))
        })?;
        if raw_buf.is_null() && len != 0 {
            return Err(PyValueError::new_err("buffer pointer is null"));
        }

        guard.disarm();
        Ok(Self { view, len })
    }

    fn ptr(&self) -> *const u8 {
        self.view.as_ref().get_ref().0.buf as *const u8
    }

    unsafe fn release(view: &mut Pin<Box<RawPyBuffer>>) {
        // Safety: called at most once from Drop; PyO3 ensures we are attached
        // to the interpreter in `Python::try_attach`.
        unsafe {
            pyo3::ffi::PyBuffer_Release(&mut Pin::get_unchecked_mut(view.as_mut()).0);
        }
    }
}

impl AsRef<[u8]> for OwnedPyBuffer {
    fn as_ref(&self) -> &[u8] {
        if self.len == 0 {
            return &[];
        }
        unsafe { slice::from_raw_parts(self.ptr(), self.len) }
    }
}

impl Drop for OwnedPyBuffer {
    fn drop(&mut self) {
        let view = &mut self.view;
        let _ = Python::try_attach(|_| unsafe { Self::release(view) });
    }
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
    #[pyo3(signature = (data, *, copy=true))]
    fn from_buffer(data: &Bound<'_, PyAny>, copy: bool) -> PyResult<Self> {
        if copy {
            if let Ok(bytes) = data.cast::<PyBytes>() {
                return Ok(Self {
                    inner: Bytes::copy_from_slice(bytes.as_bytes()),
                });
            }

            let buf = OwnedPyBuffer::get(data)?;
            return Ok(Self {
                inner: Bytes::copy_from_slice(buf.as_ref()),
            });
        }

        if let Ok(existing) = data.extract::<PyRef<'_, PacketView>>() {
            return Ok(existing.clone());
        }
        if let Ok(bytes) = data.cast::<PyBytes>() {
            return Ok(Self {
                inner: Bytes::from_owner(OwnedPyBytes::new(&bytes)),
            });
        }

        return Err(PyValueError::new_err(
            "zero-copy from_buffer requires an immutable bytes object or PacketView; use copy=True to copy from other buffers",
        ));
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
    use pyo3::types::{PyByteArray, PyModule};

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
    fn packet_view_from_buffer_zero_copy_bytes() {
        Python::attach(|py| {
            let src = PyBytes::new(py, b"hello world");
            let src_ptr = src.as_bytes().as_ptr();

            let view = PacketView::from_buffer(src.as_any(), false).expect("from_buffer");
            assert_eq!(view.__len__(), 11);
            assert_eq!(view.inner.as_ptr(), src_ptr);
        });
    }

    #[test]
    fn packet_view_from_buffer_zero_copy_rejects_readonly_view_of_mutable() {
        Python::attach(|py| {
            let src = PyByteArray::new(py, b"hello world");

            let builtins = PyModule::import(py, "builtins").expect("builtins");
            let mv = builtins
                .getattr("memoryview")
                .expect("memoryview")
                .call1((src,))
                .expect("memoryview(src)")
                .call_method0("toreadonly")
                .expect("toreadonly");

            let err = PacketView::from_buffer(mv.as_any(), false).unwrap_err();
            assert!(
                err.to_string()
                    .contains("zero-copy from_buffer requires an immutable bytes object")
            );
        });
    }

    #[test]
    fn packet_view_from_buffer_zero_copy_rejects_bytearray() {
        Python::attach(|py| {
            let src = PyByteArray::new(py, b"hello world");
            let err = PacketView::from_buffer(src.as_any(), false).unwrap_err();
            assert!(
                err.to_string()
                    .contains("zero-copy from_buffer requires an immutable bytes object")
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
