

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use eframe::egui::{Context, Ui};
use eframe::{egui, Frame, NativeOptions};
use env_logger::Env;
use uuid::Uuid;

mod data;
use crate::data::*;

const LISTEN_ADDRESS: &str = "0.0.0.0:27888";


struct Lobby {
    id: Uuid,
    round: u64,
    stage_id: Option<i32>,
    released: bool,
    players: HashMap<PlayerId, Player>,
}
impl Debug for Lobby {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lobby")
            .field("id", &self.id)
            .field("player_count", &self.players.len())
            .finish()
    }
}
impl Default for Lobby {
    fn default() -> Self {
        Self {
            id: Uuid::now_v7(),
            round: 1,
            stage_id: None,
            released: false,
            players: HashMap::new(),
        }
    }
}

#[derive(Default)]
struct ServerState {
    next_player_id: PlayerId,
    lobbies: HashMap<String, Lobby>,
}
impl ServerState {
    fn register(
        &mut self,
        lobby_code: &str,
        name: String,
        leader: bool,
        sender: ClientSender
    ) -> PlayerId {
        self.next_player_id += 1;
        let player_id = self.next_player_id;
        let lobby = self.lobbies.entry(lobby_code.to_string()).or_default();

        let player = Player {
            name,
            leader,
            ready: false,
            telemetry: None,
            checkpoints: HashMap::new(),
            finish_time_ms: None,
            _connected_at: Instant::now(),
            sender: sender.clone()
        };

        log::info!("Registered in Lobby: {:?}, PlayerId: {:?} -> {:?}", lobby, player_id, player);
        lobby.players.insert(
            player_id,
            player,
        );


        let _ = sender.send(ServerMessage::Registered {
            player_id,
            round: lobby.round,
        });
        self.broadcast_lobby_status(lobby_code);
        player_id
    }

    fn set_ready(&mut self, lobby_code: &str, player_id: PlayerId, stage_id: StageId) {
        let Some(lobby) = self.lobbies.get_mut(lobby_code) else { return };
        if lobby.stage_id.is_some_and(|current| current != stage_id) {
            lobby.round += 1;
            lobby.released = false;
            for player in lobby.players.values_mut() {
                player.ready = false;
                player.telemetry = None;
                player.checkpoints.clear();
                player.finish_time_ms = None;
            }
        }
        lobby.stage_id = Some(stage_id);
        if let Some(player) = lobby.players.get_mut(&player_id) {
            player.ready = true;
            log::info!("Set ready for PlayerId: {:?}", player);
        }
        self.broadcast_lobby_status(lobby_code);
    }

    fn start(&mut self, lobby_code: &str, player_id: PlayerId) {
        let is_leader = self.lobbies.get(lobby_code)
            .and_then(|lobby| lobby.players.get(&player_id))
            .is_some_and(|player| player.leader);
        if !is_leader {
            log::error!("Non leader player tried to start, PlayerId: {:?}", player_id);
            self.send_error(lobby_code, player_id, "Only leader can start the stage!");
            return;
        }

        let Some(lobby) = self.lobbies.get_mut(lobby_code) else { return };
        lobby.released = true;
        log::info!("Releasing lobby: {:?}", lobby);
        let message = ServerMessage::Release { round: lobby.round };
        for player in lobby.players.values() {
            let _ = player.sender.send(message.clone());
        }
    }

    fn update_telemetry(
        &mut self,
        lobby_code: &str,
        player_id: PlayerId,
        round: u64,
        telemetry: Telemetry,
    ) {
        let Some(lobby) = self.lobbies.get_mut(lobby_code) else { return };
        if round != lobby.round { return }

        let Some(player) = lobby.players.get_mut(&player_id) else { return };
        if player.telemetry.as_ref().is_some_and(|old| telemetry.sequence <= old.sequence) {
            return;
        }
        
        //log::info!("PlayerId: {:?} sent telemetry!", player_id);
        player.telemetry = Some(telemetry);
        self.broadcast_race_snapshot(lobby_code);
    }


    fn checkpoint(
        &mut self,
        lobby_code: &str,
        player_id: PlayerId,
        round: u64,
        checkpoint: u8,
        time_ms: u32,
    ) {
        let Some(lobby) = self.lobbies.get_mut(lobby_code) else { return };
        if round != lobby.round { return }

        if let Some(player) = lobby.players.get_mut(&player_id) {
            log::info!("PlayerId: {:?} crossed checkpoint: {:?} @{:?}", player_id, checkpoint, time_ms);
            player.checkpoints.insert(checkpoint, time_ms);
        }
        self.broadcast_checkpoint(lobby_code, checkpoint);
    }

    fn finish(
        &mut self,
        lobby_code: &str,
        player_id: PlayerId,
        round: u64,
        time_ms: u32
    ) {
        let Some(lobby) = self.lobbies.get_mut(lobby_code) else { return };
        if round != lobby.round { return }

        if let Some(player) = lobby.players.get_mut(&player_id) {
            log::info!("PlayerId: {:?} finished stage", player_id);
            player.finish_time_ms = Some(time_ms);
            if let Some(telemetry) = &mut player.telemetry {
                telemetry.finished = true;
            }
        }
        self.broadcast_race_snapshot(lobby_code);
    }

    fn disconnect(&mut self, lobby_code: &str, player_id: PlayerId) {
        if let Some(lobby) = self.lobbies.get_mut(lobby_code) {
            log::info!("Lobby: {:?}, removed PlayerId: {:?}", lobby, player_id);
            lobby.players.remove(&player_id);
        }
        self.broadcast_lobby_status(lobby_code);
        if self.lobbies.get(lobby_code).is_some_and(|lobby| lobby.players.is_empty()) {
            log::info!("Closed Lobby: {:?}", lobby_code);
            self.lobbies.remove(lobby_code);
        }
    }


    fn send_error(
        &self,
        lobby_code: &str,
        player_id: PlayerId,
        message: &str
    ) {
        if let Some(player) = self.lobbies.get(lobby_code).and_then(|l| l.players.get(&player_id)) {
            let _ = player.sender.send(ServerMessage::Error { message: message.into() });
        }
    }

    fn broadcast_lobby_status(
        &self,
        lobby_code: &str
    ) {
        let Some(lobby) = self.lobbies.get(lobby_code) else { return };
        let players: Vec<_> = lobby.players.iter().map(|(&id, player)| PlayerStatus {
            id,
            name: player.name.clone(),
            leader: player.leader,
            ready: player.ready,
        }).collect();

        let message = ServerMessage::LobbyStatus {
            round: lobby.round,
            stage_id: lobby.stage_id,
            players,
        };
        for player in lobby.players.values() {
            let _ = player.sender.send(message.clone());
        }
    }

    fn broadcast_race_snapshot(
        &self,
        lobby_code: &str
    ) {
        let Some(lobby) = self.lobbies.get(lobby_code) else { return };
        let leader_progress = lobby.players.values()
            .find(|player| player.leader)
            .and_then(|player| player.telemetry.as_ref())
            .map_or(0.0, |telemetry| telemetry.stage_progress);


        let mut players: Vec<_> = lobby.players.iter().filter_map(|(&id, player)| {
            let telemetry = player.telemetry.as_ref()?;
            Some(RacePlayer {
                id,
                name: player.name.clone(),
                stage_progress: telemetry.stage_progress,
                race_time_ms: telemetry.race_time_ms,
                progress_gap: telemetry.stage_progress - leader_progress,
                finished: telemetry.finished,
            })
        }).collect();
        players.sort_by(|a, b| b.stage_progress.total_cmp(&a.stage_progress));

        let message = ServerMessage::RaceSnapshot { round: lobby.round, players };
        for player in lobby.players.values() {
            let _ = player.sender.send(message.clone());
        }
    }


    fn broadcast_checkpoint(
        &self,
        lobby_code: &str,
        checkpoint: u8
    ) {
        let Some(lobby) = self.lobbies.get(lobby_code) else { return };
        let mut players: Vec<_> = lobby.players.iter().filter_map(|(&id, player)| {
            Some((id, player.name.clone(), *player.checkpoints.get(&checkpoint)?))
        }).collect();
        players.sort_by_key(|(_, _, time)| *time);
        let best = players.first().map_or(0, |(_, _, time)| *time);
        let results = players.into_iter().map(|(id, name, time_ms)| CheckpointResult {
            id,
            name,
            time_ms,
            delta_ms: time_ms.saturating_sub(best),
        }).collect();

        let message = ServerMessage::CheckpointStandings {
            round: lobby.round,
            checkpoint,
            players: results,
        };
        for player in lobby.players.values() {
            let _ = player.sender.send(message.clone());
        }
    }

    fn stage_ended(
        &mut self,
        lobby_code: &str,
        player_id: PlayerId,
    ) {
        let Some(lobby) =
            self.lobbies.get_mut(lobby_code)
        else {
            return;
        };

        if let Some(player) =
            lobby.players.get_mut(&player_id)
        {
            log::info!(
            "PlayerId: {:?} left the stage",
            player_id
        );

            player.ready = false;
            player.telemetry = None;
            player.checkpoints.clear();
            player.finish_time_ms = None;
        }

        if lobby.players.values().all(|player| !player.ready) {
            lobby.stage_id = None;
            lobby.released = false;
            lobby.round += 1;

            log::info!("Lobby {:?} race state cleared", lobby.id);
        }

        self.broadcast_lobby_status(lobby_code);
        self.broadcast_race_snapshot(lobby_code);
    }
}

struct Connection {
    lobby: String,
    player_id: PlayerId,
}

fn start_server(state: Arc<Mutex<ServerState>>) {
    thread::spawn(move || {
        let listener = TcpListener::bind(LISTEN_ADDRESS)
            .unwrap_or_else(|error| panic!("Failed to bind {LISTEN_ADDRESS}: {error}"));
        log::info!("Listening on {LISTEN_ADDRESS}");

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let state = Arc::clone(&state);
                    thread::spawn(move || handle_client(stream, state));
                }
                Err(error) => log::error!("Accept failed: {error}"),
            }
        }
    });
}

fn handle_client(stream: TcpStream, state: Arc<Mutex<ServerState>>) {
    let peer = stream.peer_addr().ok();
    let writer_stream = match stream.try_clone() {
        Ok(stream) => stream,
        Err(error) => {
            log::error!("Could not clone client stream: {error}");
            return;
        }
    };
    let (sender, receiver) = mpsc::channel::<ServerMessage>();
    thread::spawn(move || write_messages(writer_stream, receiver));

    let mut connection: Option<Connection> = None;
    for line in BufReader::new(stream).lines() {
        let Ok(line) = line else { break };
        let message = match serde_json::from_str::<ClientMessage>(&line) {
            Ok(message) => message,
            Err(error) => {
                let _ = sender.send(ServerMessage::Error { message: format!("Invalid message: {error}") });
                continue;
            }
        };

        if connection.is_none() {
            let ClientMessage::Register { lobby, player, leader } = message else {
                let _ = sender.send(ServerMessage::Error { message: "Register first".into() });
                continue;
            };
            if lobby.trim().is_empty() || player.trim().is_empty() {
                let _ = sender.send(ServerMessage::Error { message: "Lobby and player name are required".into() });
                continue;
            }
            let player_id = state.lock().unwrap().register(lobby.as_str(), player, leader, sender.clone());
            connection = Some(Connection { lobby, player_id });
            log::info!("Client {peer:?} registered as {player_id}");
            continue;
        }

        let connection = connection.as_ref().unwrap();

        let mut state = state.lock().unwrap();
        match message {
            ClientMessage::Ready { stage_id } => state.set_ready(&connection.lobby, connection.player_id, stage_id),
            ClientMessage::StartRequest => state.start(&connection.lobby, connection.player_id),
            ClientMessage::Telemetry { round, sequence, stage_progress, race_time_ms, split_reached, finished } => {
                state.update_telemetry(&connection.lobby, connection.player_id, round, Telemetry {
                    sequence, stage_progress, race_time_ms, split_reached, finished,
                });
            }
            ClientMessage::Checkpoint { round, checkpoint, time_ms } => {
                state.checkpoint(&connection.lobby, connection.player_id, round, checkpoint, time_ms);
            }
            ClientMessage::Finish { round, time_ms } => {
                state.finish(&connection.lobby, connection.player_id, round, time_ms);
            }
            ClientMessage::Ping => { let _ = sender.send(ServerMessage::Pong); }
            ClientMessage::Register { .. } => unreachable!(),
            ClientMessage::StageEnded => {
                state.stage_ended(
                    &connection.lobby,
                    connection.player_id,
                );
            }
        }
    }

    if let Some(connection) = connection {
        state.lock().unwrap().disconnect(&connection.lobby, connection.player_id);
    }
    log::info!("Client {peer:?} disconnected");
}

fn write_messages(mut stream: TcpStream, receiver: mpsc::Receiver<ServerMessage>) {
    for message in receiver {
        let Ok(mut json) = serde_json::to_string(&message) else { continue };
        json.push('\n');
        if stream.write_all(json.as_bytes()).is_err() { break }
    }
}

struct App {
    state: Arc<Mutex<ServerState>>,
}

impl App {
    fn new(_cc: &eframe::CreationContext<'_>, state: Arc<Mutex<ServerState>>) -> Self {
        Self { state }
    }

    fn render(&self, ui: &mut Ui) {
        ui.heading("PartyRSF Server");
        ui.label(format!("Listening on {LISTEN_ADDRESS}"));
        ui.separator();

        let state = self.state.lock().unwrap();
        if state.lobbies.is_empty() {
            ui.label("No active lobbies");
            return;
        }

        for (code, lobby) in &state.lobbies {
            egui::CollapsingHeader::new(format!("{code} — round {}", lobby.round))
                .default_open(true)
                .show(ui, |ui| {
                    ui.label(format!("Stage: {:?} | Released: {}", lobby.stage_id, lobby.released));
                    egui::Grid::new(format!("players_{code}")).striped(true).show(ui, |ui| {
                        ui.strong("Player"); ui.strong("Role"); ui.strong("Ready");
                        ui.strong("Progress"); ui.strong("Time"); ui.end_row();
                        for player in lobby.players.values() {
                            ui.label(&player.name);
                            ui.label(if player.leader { "Leader" } else { "Player" });
                            ui.label(if player.ready { "Yes" } else { "No" });
                            ui.label(player.telemetry.as_ref().map_or("—".into(), |t| format!("{:.1}", t.stage_progress)));
                            ui.label(player.telemetry.as_ref().map_or("—".into(), |t| format!("{:.3}s", t.race_time_ms as f32 / 1000.0)));
                            ui.end_row();
                        }
                    });
                });
        }
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &Context, _frame: &mut Frame) {
        egui::CentralPanel::default().show(ctx, |ui| self.render(ui));
        ctx.request_repaint_after(Duration::from_millis(10));
    }
}

fn main() -> eframe::Result {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    log::info!("PartyRSF server started");

    let state = Arc::new(Mutex::new(ServerState::default()));
    start_server(Arc::clone(&state));

    eframe::run_native(
        "PartyRSF Server",
        NativeOptions::default(),
        Box::new(move |cc| Ok(Box::new(App::new(cc, Arc::clone(&state))))),
    )
}
