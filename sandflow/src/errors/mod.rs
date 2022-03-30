use std::error::Error;
use std::fmt::{Display, Formatter};

use futures::channel::mpsc::SendError;

/// means Fatal Error;
#[derive(Debug)]
pub enum FError {
    SystemIO(std::io::Error),
    StrHint(String),
    ChSend(SendError),
    Unknown(Box<dyn Error + Send + Sync + 'static>),
}

impl Display for FError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FError::SystemIO(e) => write!(f, "{}", e),
            FError::StrHint(str) => f.write_str(str),
            FError::ChSend(e) => write!(f, "{}", e),
            FError::Unknown(e) => write!(f, "{}", e),
        }
    }
}

impl Error for FError {}

impl From<std::io::Error> for FError {
    fn from(e: std::io::Error) -> Self {
        FError::SystemIO(e)
    }
}

impl From<SendError> for FError {
    fn from(e: SendError) -> Self {
        FError::ChSend(e)
    }
}
