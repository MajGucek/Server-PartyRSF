use std::collections::HashMap;
use std::sync::mpsc;
use std::time::Instant;
use serde;
use serde::{Deserialize, Serialize};

pub type PlayerId = u64;
pub type StageId = i32;
pub type ClientSender = mpsc::Sender<ServerMessage>;


#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Register {
        lobby: String,
        player: String,
        leader: bool,
    },
    Ready {
        stage_id: StageId,
    },
    StartRequest,
    Telemetry {
        round: u64,
        sequence: u64,
        stage_progress: f32,
        race_time_ms: u32,
        split_reached: u8,
        finished: bool,
    },
    Checkpoint {
        round: u64,
        checkpoint: u8,
        time_ms: u32,
    },
    Finish {
        round: u64,
        time_ms: u32,
    },
    Ping,
    StageEnded,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    Registered {
        player_id: PlayerId,
        round: u64,
    },
    LobbyStatus {
        round: u64,
        stage_id: Option<StageId>,
        players: Vec<PlayerStatus>,
    },
    Release {
        round: u64,
    },
    RaceSnapshot {
        round: u64,
        players: Vec<RacePlayer>,
    },
    CheckpointStandings {
        round: u64,
        checkpoint: u8,
        players: Vec<CheckpointResult>,
    },
    Error {
        message: String,
    },
    Pong,
}

#[derive(Clone, Debug, Serialize)]
pub struct PlayerStatus {
    pub id: PlayerId,
    pub name: String,
    pub leader: bool,
    pub ready: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct RacePlayer {
    pub id: PlayerId,
    pub name: String,
    pub stage_progress: f32,
    pub race_time_ms: u32,
    pub progress_gap: f32,
    pub finished: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct CheckpointResult {
    pub id: PlayerId,
    pub name: String,
    pub time_ms: u32,
    pub delta_ms: u32,
}

#[derive(Clone, Debug, Default)]
pub struct Telemetry {
    pub sequence: u64,
    pub stage_progress: f32,
    pub race_time_ms: u32,
    pub split_reached: u8,
    pub finished: bool,
}

#[derive(Debug)]
pub struct Player {
    pub name: String,
    pub leader: bool,
    pub ready: bool,
    pub telemetry: Option<Telemetry>,
    pub checkpoints: HashMap<u8, u32>,
    pub finish_time_ms: Option<u32>,
    pub _connected_at: Instant,
    pub sender: ClientSender,
}