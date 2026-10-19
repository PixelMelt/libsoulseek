use std::future::Future;
use std::pin::Pin;

use tokio::io::{AsyncRead, AsyncWrite};

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

pub type FileReader = Box<dyn AsyncRead + Send + Unpin>;

pub type FileWriter = Box<dyn AsyncWrite + Send + Unpin>;
