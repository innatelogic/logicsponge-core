#![warn(clippy::all)]

use pyo3::exceptions::{PyIndexError, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::{PyObject, Python};

pub mod channel;

/// A wrapper around `PyObject` that implements `Clone` safely,
/// requiring the Python GIL at clone time.
#[derive(Debug)]
pub struct ClonePyObject {
    inner: PyObject,
}

impl ClonePyObject {
    pub fn new(obj: PyObject) -> Self {
        Self { inner: obj }
    }

    pub fn get(&self) -> &PyObject {
        &self.inner
    }

    /// Extract the inner PyObject by value
    pub fn into_inner(self) -> PyObject {
        self.inner
    }
}

impl Clone for ClonePyObject {
    fn clone(&self) -> Self {
        Python::with_gil(|py| Self {
            inner: self.inner.clone_ref(py),
        })
    }
}

#[pyclass]
pub struct Sender {
    inner: channel::Sender<ClonePyObject>,
}

#[pymethods]
impl Sender {
    pub fn send(&self, obj: PyObject) -> PyResult<()> {
        match self.inner.send(ClonePyObject::new(obj)) {
            Ok(()) => Ok(()),
            Err(_error) => Err(PyRuntimeError::new_err("Channel disconnected")),
        }
    }
}

#[pyclass]
pub struct Receiver {
    inner: channel::Receiver<ClonePyObject>,
}

#[pymethods]
impl Receiver {
    pub fn recv(&self, py: Python<'_>) -> PyResult<PyObject> {
        let msg = py.allow_threads(|| self.inner.recv());
        match msg {
            Ok(obj) => Ok(obj.into_inner()),
            Err(_error) => Err(PyRuntimeError::new_err("Channel disconnected")),
        }
    }

    pub fn try_recv(&self) -> PyResult<Option<PyObject>> {
        match self.inner.try_recv() {
            Ok(obj) => Ok(Some(obj.into_inner())),
            Err(channel::TryRecvError::Empty) => Ok(None),
            Err(channel::TryRecvError::Disconnected) => {
                Err(PyRuntimeError::new_err("Channel disconnected"))
            }
        }
    }

    pub fn __len__(&self) -> PyResult<usize> {
        Ok(self.inner.len())
    }

    pub fn history(&self, index: usize) -> PyResult<PyObject> {
        match self.inner.history(index) {
            Ok(obj) => Ok(obj.into_inner()),
            Err(_error) => Err(PyIndexError::new_err("Invalid index")),
        }
    }

    pub fn to_list(&self) -> Vec<PyObject> {
        self.inner
            .to_list()
            .into_iter()
            .map(|obj| obj.into_inner())
            .collect()
    }
}

#[pyfunction]
pub fn make_channel() -> (Sender, Receiver) {
    let (tx_inner, rx_inner) = channel::channel();
    (Sender { inner: tx_inner }, Receiver { inner: rx_inner })
}

#[pymodule]
fn core_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(make_channel, m)?)?;
    m.add_class::<Sender>()?;
    m.add_class::<Receiver>()?;
    Ok(())
}
