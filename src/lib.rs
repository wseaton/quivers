extern crate redis;

use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::wrap_pyfunction;
use pyo3::Python;

use std::io::{copy, Cursor, Read};

use redis::Commands;
use redis::Connection;

// #[pyfunction]
// fn check_callable<'a>(
//     py: Python,
//     func: &'a PyAny,
//     args: &PyTuple,
//     kwargs: Option<&PyDict>,
// ) -> PyResult<&'a PyAny> {
//     if func.is_callable() {
//         let func_name = func
//             .getattr("__name__")
//             .unwrap()
//             .downcast::<PyString>()
//             .unwrap()
//             .to_owned();
//         let result = func.call(args, kwargs).unwrap();

//         Ok(result)
//     } else {
//         Err(PyValueError::new_err("Function must be callable."))
//     }
//

fn get_redis_connection(uri: String) -> Connection {
    let client = redis::Client::open(uri).unwrap();
    let con = client.get_connection().unwrap();
    con
}

#[pyfunction]
fn stash_bytes(uri: String, bytes: Vec<u8>, key: String, expires: usize) -> PyResult<()> {
    let mut buff = Cursor::new(bytes);

    let output_buffer = Cursor::new(vec![]);
    let mut wtr = snap::write::FrameEncoder::new(output_buffer);

    copy(&mut buff, &mut wtr).expect("I/O operation failed");
    let bytes = wtr.into_inner().unwrap().into_inner();

    let mut r = get_redis_connection(uri);

    // throw away the result, just make sure it does not fail
    let _: () = r.set_ex(key, bytes, expires).unwrap();

    Ok(())
}

#[pyfunction]
fn get_bytes(py: Python, uri: String, key: String) -> PyResult<&PyBytes> {
    let mut r = get_redis_connection(uri);

    let value: Vec<u8> = r.get(key).unwrap();

    let mut buf = vec![];
    snap::read::FrameDecoder::new(value.as_slice())
        .read_to_end(&mut buf)
        .unwrap();

    let s = buf.as_slice();

    Ok(PyBytes::new(py, s).into())
}

#[pymodule]
fn quivers(_py: Python, m: &PyModule) -> PyResult<()> {
    // m.add_function(wrap_pyfunction!(check_callable, m)?)?;
    m.add_function(wrap_pyfunction!(stash_bytes, m)?)?;
    m.add_function(wrap_pyfunction!(get_bytes, m)?)?;
    Ok(())
}
