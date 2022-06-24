pub mod bus;
pub mod conn;
pub mod route;
pub mod service;

use conn::*;
use lazy_static::*;

lazy_static! {
    pub static ref CONN_MANAGER: ConnManager = ConnManager::new();
}
