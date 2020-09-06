use std::sync::{Mutex, RwLock, MutexGuard, RwLockWriteGuard, RwLockReadGuard};

pub(crate) fn guard<T>(lock: &Mutex<T>) -> MutexGuard<T> {
    match lock.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub(crate) fn write_guard<T>(lock: &RwLock<T>) -> RwLockWriteGuard<T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

pub(crate) fn read_guard<T>(lock: &RwLock<T>) -> RwLockReadGuard<T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}