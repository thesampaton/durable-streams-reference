#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod config;
pub mod handlers;
pub mod middleware;
pub mod protocol;
pub mod router;
pub mod storage;
