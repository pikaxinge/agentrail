use thiserror::Error;

#[derive(Debug, Error)]
pub enum CoreError {
    #[error("phase not found: {0}")]
    PhaseNotFound(String),
    #[error("step not found: {0}")]
    StepNotFound(String),
    #[error("invalid transition: {0}")]
    InvalidTransition(String),
}
