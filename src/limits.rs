use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub enum ConcurrencyPermit {
    Unbounded,
    Limited(OwnedSemaphorePermit),
}

pub struct ConcurrencyLimiter {
    semaphore: Option<Arc<Semaphore>>,
}

impl ConcurrencyLimiter {
    pub fn new(limit: usize) -> Self {
        if limit == 0 {
            Self { semaphore: None }
        } else {
            Self {
                semaphore: Some(Arc::new(Semaphore::new(limit))),
            }
        }
    }

    pub fn try_acquire(&self) -> Option<ConcurrencyPermit> {
        match &self.semaphore {
            Some(semaphore) => semaphore
                .clone()
                .try_acquire_owned()
                .ok()
                .map(ConcurrencyPermit::Limited),
            None => Some(ConcurrencyPermit::Unbounded),
        }
    }
}
