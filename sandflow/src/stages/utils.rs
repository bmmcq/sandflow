use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::FError;

pub struct ErrorHook {
    has_error: AtomicBool,
    error: UnsafeCell<Option<FError>>,
}

impl ErrorHook {
    pub fn new() -> Self {
        Self { has_error: AtomicBool::new(false), error: UnsafeCell::new(None) }
    }

    pub fn set_error(&self, error: FError) -> Option<FError> {
        if self
            .has_error
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            unsafe { &mut *self.error.get() }.replace(error);
            None
        } else {
            Some(error)
        }
    }

    pub fn take_error(&self) -> Option<FError> {
        if self.has_error.load(Ordering::SeqCst) {
            unsafe { &mut *self.error.get() }.take()
        } else {
            None
        }
    }

    #[inline]
    pub fn has_error(&self) -> bool {
        self.has_error.load(Ordering::SeqCst)
    }
}

unsafe impl Send for ErrorHook {}
unsafe impl Sync for ErrorHook {}
