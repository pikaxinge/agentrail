use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PhaseStatus {
    Locked,
    Pending,
    InProgress,
    Done,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Claimed,
    Done,
    Skipped,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Step {
    pub id: String,
    pub name: String,
    pub status: StepStatus,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub claimed_by: Option<String>,
    pub evidence: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Phase {
    pub id: String,
    pub name: String,
    pub status: PhaseStatus,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Plan {
    #[serde(default = "default_version")]
    pub version: u32,
    pub project: String,
    #[serde(default)]
    pub phases: Vec<Phase>,
}

fn default_version() -> u32 {
    1
}
