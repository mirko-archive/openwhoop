use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, Local};
use futures::{Stream, StreamExt};
use openwhoop_codec::{
    DataRangeInfo, GetDataRangeResponse, HistoryReading, WhoopCommandResponse, WhoopData,
    WhoopPacket,
    constants::{CMD_FROM_STRAP_GEN5, DATA_FROM_STRAP_GEN5, EVENTS_FROM_STRAP_GEN5, MetadataType},
};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::time::{sleep, timeout};

use crate::ble::{BleNotification, WhoopBleTransport};

use super::{HistorySyncConfig, WhoopDeviceWith};

enum NextPacketError {
    IdleTimeout(Duration),
    Other(anyhow::Error),
}

impl NextPacketError {
    fn into_anyhow(self) -> anyhow::Error {
        match self {
            Self::IdleTimeout(wait_for) => {
                anyhow!(
                    "No incoming packets for {:.1}s (idle timeout)",
                    wait_for.as_secs_f32()
                )
            }
            Self::Other(err) => err,
        }
    }
}

pub(super) struct Gen5HistorySync<'a, T, S>
where
    T: WhoopBleTransport,
    S: Stream<Item = BleNotification> + Unpin,
{
    device: &'a mut WhoopDeviceWith<T>,
    should_exit: Arc<AtomicBool>,
    notifications: S,
    config: HistorySyncConfig,
    queued_packets: VecDeque<WhoopPacket>,
    pending_readings: Vec<HistoryReading>,
    saw_stream_activity: bool,
    saw_non_history_packets: bool,
    recovery_attempted: bool,
    history_complete: bool,
    history_started: bool,
    last_range_info: Option<DataRangeInfo>,
    get_data_range_warned_short: bool,
}

impl<'a, T, S> Gen5HistorySync<'a, T, S>
where
    T: WhoopBleTransport,
    S: Stream<Item = BleNotification> + Unpin,
{
    pub(super) fn new(
        device: &'a mut WhoopDeviceWith<T>,
        should_exit: Arc<AtomicBool>,
        notifications: S,
        config: HistorySyncConfig,
    ) -> Self {
        Self {
            device,
            should_exit,
            notifications,
            config,
            queued_packets: VecDeque::new(),
            pending_readings: Vec::new(),
            saw_stream_activity: false,
            saw_non_history_packets: false,
            recovery_attempted: false,
            history_complete: false,
            history_started: false,
            last_range_info: None,
            get_data_range_warned_short: false,
        }
    }

    pub(super) async fn start(mut self) -> anyhow::Result<()> {
        let result = self.run().await;

        if let Err(err) = result {
            if self.history_started {
                match self.device.is_connected().await {
                    Ok(false) => {
                        warn!(
                            "Gen5 history transfer failed after disconnect; skipping best-effort failure and abort: {err}"
                        );
                    }
                    Ok(true) => {
                        warn!(
                            "Gen5 history transfer failed; sending best-effort failure and abort: {err}"
                        );
                        let _ = self
                            .device
                            .send_command(WhoopPacket::history_end_failure())
                            .await;
                        let _ = self
                            .device
                            .send_command(WhoopPacket::abort_historical_transmits())
                            .await;
                    }
                    Err(check_err) => {
                        warn!(
                            "Gen5 history transfer failed; connection check failed ({check_err}), attempting best-effort failure and abort: {err}"
                        );
                        let _ = self
                            .device
                            .send_command(WhoopPacket::history_end_failure())
                            .await;
                        let _ = self
                            .device
                            .send_command(WhoopPacket::abort_historical_transmits())
                            .await;
                    }
                }
            }
            return Err(err);
        }

        Ok(())
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
        let stream_timeout = self.config.overall_timeout;
        let idle_timeout = self.config.idle_timeout;

        match self
            .send_command_wait_response(WhoopPacket::get_data_range_gen5(), COMMAND_TIMEOUT, true)
            .await
        {
            Ok(resp) => match resp.get_data_range_response() {
                Some(GetDataRangeResponse::Range(range)) => {
                    self.last_range_info = Some(range);
                }
                Some(GetDataRangeResponse::ShortPayload) if !self.get_data_range_warned_short => {
                    warn!(
                        "GetDataRange returned short payload (len=3); range fields unavailable on this firmware"
                    );
                    self.get_data_range_warned_short = true;
                }
                Some(GetDataRangeResponse::UnrecognizedPayload { len }) => {
                    trace!(
                        "GetDataRange response body shape not recognized (len={})",
                        len
                    );
                }
                _ => {}
            },
            Err(err) => {
                warn!("GetDataRange preflight failed: {err}");
            }
        }

        let start = self
            .send_command_wait_response(WhoopPacket::history_start_gen5(), COMMAND_TIMEOUT, false)
            .await?;

        self.history_started = true;
        if !matches!(start.result, 1 | 2) {
            bail!(
                "SendHistoricalData rejected: {} ({})",
                start.result,
                start.result_name()
            );
        }

        let mut started_at = Instant::now();
        while !self.should_exit.load(Ordering::SeqCst) {
            let elapsed = started_at.elapsed();
            if let Some(stream_timeout) = stream_timeout {
                if elapsed >= stream_timeout {
                    bail!(
                        "Historical transfer timed out after {} seconds",
                        stream_timeout.as_secs()
                    );
                }
            }

            let wait_for = match stream_timeout {
                Some(stream_timeout) => {
                    let remaining = stream_timeout.saturating_sub(elapsed);
                    idle_timeout.min(remaining)
                }
                None => idle_timeout,
            };
            let packet = match self.next_packet(wait_for).await {
                Ok(Some(packet)) => packet,
                Ok(None) => break,
                Err(NextPacketError::IdleTimeout(_))
                    if self.retry_after_idle_timeout(COMMAND_TIMEOUT).await? =>
                {
                    started_at = Instant::now();
                    continue;
                }
                Err(NextPacketError::IdleTimeout(_)) if !self.saw_stream_activity => {
                    if self.saw_non_history_packets {
                        if let Some(range) = self.last_range_info {
                            bail!(
                                "Received non-historical packets after SendHistoricalData but no history rows/metadata; data_range_distance={} (start={}, end={}, rollover={})",
                                range.distance,
                                range.start,
                                range.end,
                                range.rollover
                            );
                        }
                        bail!(
                            "Received non-historical packets after SendHistoricalData but no history rows/metadata"
                        );
                    }

                    if self.recovery_attempted {
                        if let Some(range) = self.last_range_info {
                            bail!(
                                "No historical packets observed after SendHistoricalData even after abort/retry; data_range_distance={} (start={}, end={}, rollover={})",
                                range.distance,
                                range.start,
                                range.end,
                                range.rollover
                            );
                        }
                        bail!(
                            "No historical packets observed after SendHistoricalData even after abort/retry"
                        );
                    }

                    if let Some(range) = self.last_range_info {
                        info!(
                            "No historical packets observed after SendHistoricalData; data_range_distance={} (start={}, end={}, rollover={})",
                            range.distance, range.start, range.end, range.rollover
                        );
                    } else {
                        info!(
                            "No historical packets observed after SendHistoricalData; treating as empty history"
                        );
                    }
                    break;
                }
                Err(err) => {
                    self.persist_pending_readings(
                        "failed to persist trailing historical readings after transfer error",
                    )
                    .await?;
                    return Err(err.into_anyhow());
                }
            };

            let Ok(data) = WhoopData::from_packet_gen5(packet) else {
                self.saw_non_history_packets = true;
                trace!("Unhandled Gen5 packet during history transfer");
                continue;
            };

            match data {
                WhoopData::CommandResponse(_) => {
                    self.saw_non_history_packets = true;
                }
                WhoopData::HistoryMetadata { cmd, end_data, .. } => {
                    self.saw_stream_activity = true;
                    match cmd {
                        MetadataType::HistoryStart => {
                            trace!("Gen5 history metadata start");
                        }
                        MetadataType::HistoryEnd => {
                            if !self.pending_readings.is_empty() {
                                self.persist_pending_readings(
                                    "failed to persist historical readings",
                                )
                                .await?;
                            }

                            match self
                                .send_command_wait_response(
                                    WhoopPacket::history_end(end_data),
                                    COMMAND_TIMEOUT,
                                    false,
                                )
                                .await
                            {
                                Ok(ack) if matches!(ack.result, 1 | 2) => {}
                                Ok(ack) => {
                                    warn!(
                                        "HistoryEnd ack returned {} ({})",
                                        ack.result,
                                        ack.result_name()
                                    );
                                }
                                Err(err) => {
                                    warn!("HistoryEnd ack failed: {err}");
                                    let _ = self
                                        .device
                                        .send_command(WhoopPacket::history_end_failure())
                                        .await;
                                }
                            }
                        }
                        MetadataType::HistoryComplete => {
                            self.history_complete = true;
                            break;
                        }
                    }
                }
                WhoopData::HistoryReading(reading) => {
                    self.saw_stream_activity = true;
                    let ptime = DateTime::from_timestamp_millis(i64::try_from(reading.unix)?)
                        .map(|t| {
                            t.with_timezone(&Local)
                                .format("%Y-%m-%d %H:%M:%S")
                                .to_string()
                        })
                        .unwrap_or_else(|| reading.unix.to_string());
                    info!(target: "HistoryReading", "time: {}", ptime);
                    self.pending_readings.push(reading);
                }
                WhoopData::ConsoleLog { .. }
                | WhoopData::RunAlarm { .. }
                | WhoopData::Event { .. }
                | WhoopData::UnknownEvent { .. }
                | WhoopData::VersionInfo { .. }
                | WhoopData::RealtimeHr { .. }
                | WhoopData::AlarmInfo { .. } => {
                    self.saw_non_history_packets = true;
                }
            }
        }

        self.persist_pending_readings("failed to persist trailing historical readings")
            .await?;

        if self.should_exit.load(Ordering::SeqCst) {
            return Ok(());
        }

        if !self.history_complete && self.saw_stream_activity {
            bail!("Transfer ended without HistoryComplete metadata");
        }

        Ok(())
    }

    async fn retry_after_idle_timeout(
        &mut self,
        command_timeout: Duration,
    ) -> anyhow::Result<bool> {
        if self.recovery_attempted || self.should_exit.load(Ordering::SeqCst) {
            return Ok(false);
        }

        self.recovery_attempted = true;
        self.persist_pending_readings(
            "failed to persist trailing historical readings before recovery retry",
        )
        .await?;

        if let Some(range) = self.last_range_info {
            warn!(
                "Gen5 history sync stalled; aborting and retrying once (data_range_distance={} start={} end={} rollover={})",
                range.distance, range.start, range.end, range.rollover
            );
        } else {
            warn!("Gen5 history sync stalled; aborting and retrying once");
        }

        self.queued_packets.clear();

        match self
            .send_command_wait_response(
                WhoopPacket::abort_historical_transmits(),
                command_timeout,
                false,
            )
            .await
        {
            Ok(resp) if matches!(resp.result, 1 | 2) => {}
            Ok(resp) => {
                warn!(
                    "AbortHistoricalTransmits ack returned {} ({}) during recovery retry",
                    resp.result,
                    resp.result_name()
                );
            }
            Err(err) => {
                warn!("AbortHistoricalTransmits failed during recovery retry: {err}");
            }
        }

        sleep(Duration::from_secs(3)).await;

        let start = self
            .send_command_wait_response(WhoopPacket::history_start_gen5(), command_timeout, false)
            .await?;

        if !matches!(start.result, 1 | 2) {
            bail!(
                "SendHistoricalData rejected during recovery retry: {} ({})",
                start.result,
                start.result_name()
            );
        }

        Ok(true)
    }

    async fn persist_pending_readings(&mut self, context: &'static str) -> anyhow::Result<()> {
        if self.pending_readings.is_empty() {
            return Ok(());
        }

        self.device
            .whoop
            .database
            .create_readings(std::mem::take(&mut self.pending_readings))
            .await
            .context(context)?;

        Ok(())
    }

    async fn stream_closed_error(&mut self, phase: &'static str) -> anyhow::Error {
        match self.device.is_connected().await {
            Ok(false) => anyhow!("Whoop disconnected while {phase}"),
            Ok(true) => anyhow!("notification stream ended unexpectedly while {phase}"),
            Err(err) => anyhow!(
                "notification stream ended unexpectedly while {phase}; failed to determine connection state: {err}"
            ),
        }
    }

    async fn send_command_wait_response(
        &mut self,
        packet: WhoopPacket,
        response_timeout: Duration,
        wait_for_non_pending: bool,
    ) -> anyhow::Result<WhoopCommandResponse> {
        let expected_cmd = packet.cmd;
        let expected_seq = self.device.send_command_with_seq(packet).await?;
        let deadline = Instant::now() + response_timeout;
        let mut pending_response: Option<WhoopCommandResponse> = None;

        loop {
            let now = Instant::now();
            if now >= deadline {
                if let Some(resp) = pending_response {
                    return Ok(resp);
                }
                bail!(
                    "timed out waiting for command response cmd={} seq={}",
                    expected_cmd,
                    expected_seq
                );
            }

            let wait_for = deadline.saturating_duration_since(now);
            let notification =
                timeout(wait_for, self.notifications.next())
                    .await
                    .map_err(|_| {
                        anyhow!(
                            "timed out waiting for command response cmd={} seq={}",
                            expected_cmd,
                            expected_seq
                        )
                    })?;
            let notification = match notification {
                Some(notification) => notification,
                None => {
                    return Err(self
                        .stream_closed_error("waiting for a command response")
                        .await);
                }
            };

            let Some(packet) = self.decode_notification(notification).await? else {
                continue;
            };
            let Ok(resp) = WhoopCommandResponse::from_packet(&packet) else {
                self.queued_packets.push_back(packet);
                continue;
            };

            if resp.cmd == expected_cmd && resp.origin_seq == expected_seq {
                if wait_for_non_pending && resp.result == 2 {
                    pending_response = Some(resp);
                    continue;
                }
                return Ok(resp);
            }

            self.queued_packets.push_back(packet);
            trace!(
                "Ignoring unmatched command response cmd={} origin_seq={} result={}",
                resp.cmd, resp.origin_seq, resp.result
            );
        }
    }

    async fn decode_notification(
        &self,
        notification: BleNotification,
    ) -> anyhow::Result<Option<WhoopPacket>> {
        let packet = self.device.notification_to_model(notification).await?;
        match packet.uuid {
            uuid if uuid == CMD_FROM_STRAP_GEN5
                || uuid == DATA_FROM_STRAP_GEN5
                || uuid == EVENTS_FROM_STRAP_GEN5 =>
            {
                match WhoopPacket::from_data_maverick(packet.bytes) {
                    Ok(packet) => Ok(Some(packet)),
                    Err(err) => {
                        trace!("Failed to parse Maverick frame: {}", err);
                        Ok(None)
                    }
                }
            }
            _ => Ok(None),
        }
    }

    async fn next_packet(
        &mut self,
        wait_for: Duration,
    ) -> Result<Option<WhoopPacket>, NextPacketError> {
        if let Some(packet) = self.queued_packets.pop_front() {
            return Ok(Some(packet));
        }

        let deadline = Instant::now() + wait_for;
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(NextPacketError::IdleTimeout(wait_for));
            }

            let remaining = deadline.saturating_duration_since(now);
            let notification = match timeout(remaining, self.notifications.next()).await {
                Ok(Some(notification)) => notification,
                Ok(None) => {
                    return Err(NextPacketError::Other(
                        self.stream_closed_error("receiving historical packets")
                            .await,
                    ));
                }
                Err(_) => return Err(NextPacketError::IdleTimeout(wait_for)),
            };

            if let Some(packet) = self
                .decode_notification(notification)
                .await
                .map_err(NextPacketError::Other)?
            {
                return Ok(Some(packet));
            }
        }
    }
}
