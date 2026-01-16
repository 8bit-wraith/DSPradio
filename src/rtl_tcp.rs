//! rtl_tcp client module
//!
//! This module provides client implementations for connecting to remote RTL-SDR
//! devices via the rtl_tcp protocol. rtl_tcp is a simple protocol that allows
//! streaming I/Q samples from an RTL-SDR device over TCP.
//!
//! # Protocol Overview
//!
//! The rtl_tcp protocol works as follows:
//! 1. Client connects to server (default port 1234)
//! 2. Server sends 12-byte handshake: magic "RTL0" + tuner_type (u32 BE) + gain_count (u32 BE)
//! 3. Client sends 5-byte commands: cmd_id (u8) + value (u32 BE)
//! 4. Server streams raw Cu8 I/Q samples continuously
//!
//! # Examples
//!
//! ## Synchronous Usage
//!
//! ```no_run
//! use dsp_radio::rtl_tcp::{RtlTcpReader, RtlTcpConfig};
//! use dsp_radio::Gain;
//!
//! let config = RtlTcpConfig {
//!     host: "192.168.1.100".to_string(),
//!     port: 1234,
//!     center_freq: 1090_000_000,
//!     sample_rate: 2_400_000,
//!     gain: Gain::Auto,
//!     bias_tee: false,
//!     ppm_correction: 0,
//! };
//!
//! let mut reader = RtlTcpReader::new(&config)?;
//! println!("Connected to {:?} tuner", reader.dongle_info().tuner_type);
//!
//! for chunk in reader {
//!     let samples = chunk?;
//!     println!("Received {} samples", samples.len());
//! }
//! # Ok::<(), dsp_radio::Error>(())
//! ```
//!
//! ## Asynchronous Usage
//!
//! ```no_run
//! use dsp_radio::rtl_tcp::{AsyncRtlTcpReader, RtlTcpConfig};
//! use dsp_radio::Gain;
//! use futures::StreamExt;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<(), dsp_radio::Error> {
//! let config = RtlTcpConfig {
//!     host: "192.168.1.100".to_string(),
//!     port: 1234,
//!     center_freq: 1090_000_000,
//!     sample_rate: 2_400_000,
//!     gain: Gain::Auto,
//!     bias_tee: false,
//!     ppm_correction: 0,
//! };
//!
//! let mut reader = AsyncRtlTcpReader::new(&config)?;
//!
//! while let Some(chunk) = reader.next().await {
//!     let samples = chunk?;
//!     println!("Received {} samples", samples.len());
//! }
//! # Ok(())
//! # }
//! ```

use std::io::{Read, Write};
use std::net::TcpStream;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use num_complex::Complex;

use crate::error::{Error, Result};
use crate::Gain;

/// rtl_tcp protocol constants
pub mod protocol {
    /// Magic bytes at start of handshake
    pub const MAGIC: &[u8; 4] = b"RTL0";

    /// Default rtl_tcp server port
    pub const DEFAULT_PORT: u16 = 1234;

    /// Default buffer size for I/Q samples (16KB = 8K samples)
    pub const DEFAULT_BUF_SIZE: usize = 16384;

    /// Connection timeout in seconds
    pub const CONNECT_TIMEOUT_SECS: u64 = 10;

    // Command IDs (5-byte packets: cmd_id + u32 BE value)

    /// Set center frequency in Hz
    pub const CMD_SET_FREQUENCY: u8 = 0x01;
    /// Set sample rate in Hz
    pub const CMD_SET_SAMPLE_RATE: u8 = 0x02;
    /// Set gain mode: 0=auto, 1=manual
    pub const CMD_SET_GAIN_MODE: u8 = 0x03;
    /// Set tuner gain in tenths of dB (manual mode)
    pub const CMD_SET_GAIN: u8 = 0x04;
    /// Set frequency correction in PPM
    pub const CMD_SET_FREQ_CORRECTION: u8 = 0x05;
    /// Set IF gain stage (stage << 16 | gain_tenths_db)
    pub const CMD_SET_IF_GAIN: u8 = 0x06;
    /// Enable/disable test mode
    pub const CMD_SET_TEST_MODE: u8 = 0x07;
    /// Set AGC mode: 0=off, 1=on
    pub const CMD_SET_AGC_MODE: u8 = 0x08;
    /// Set direct sampling mode: 0=off, 1=I-ADC, 2=Q-ADC
    pub const CMD_SET_DIRECT_SAMPLING: u8 = 0x09;
    /// Set offset tuning: 0=off, 1=on
    pub const CMD_SET_OFFSET_TUNING: u8 = 0x0A;
    /// Set RTL xtal frequency
    pub const CMD_SET_RTL_XTAL: u8 = 0x0B;
    /// Set tuner xtal frequency
    pub const CMD_SET_TUNER_XTAL: u8 = 0x0C;
    /// Set tuner gain by index
    pub const CMD_SET_GAIN_BY_INDEX: u8 = 0x0D;
    /// Set bias tee: 0=off, 1=on
    pub const CMD_SET_BIAS_TEE: u8 = 0x0E;
}

/// RTL-SDR tuner type from handshake
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum TunerType {
    /// Unknown tuner
    Unknown = 0,
    /// Elonics E4000
    E4000 = 1,
    /// Fitipower FC0012
    FC0012 = 2,
    /// Fitipower FC0013
    FC0013 = 3,
    /// FCI FC2580
    FC2580 = 4,
    /// Rafael Micro R820T
    R820T = 5,
    /// Rafael Micro R828D
    R828D = 6,
}

impl TunerType {
    /// Parse tuner type from u32 value
    pub fn from_u32(value: u32) -> Self {
        match value {
            1 => TunerType::E4000,
            2 => TunerType::FC0012,
            3 => TunerType::FC0013,
            4 => TunerType::FC2580,
            5 => TunerType::R820T,
            6 => TunerType::R828D,
            _ => TunerType::Unknown,
        }
    }
}

/// Information about the remote RTL-SDR dongle
#[derive(Debug, Clone)]
pub struct RtlTcpDongleInfo {
    /// Tuner chip type
    pub tuner_type: TunerType,
    /// Number of available gain values
    pub gain_count: u32,
}

/// Configuration for rtl_tcp client connection
#[derive(Debug, Clone, PartialEq)]
pub struct RtlTcpConfig {
    /// Remote host address
    pub host: String,
    /// Remote port (default: 1234)
    pub port: u16,
    /// Center frequency in Hz
    pub center_freq: u32,
    /// Sample rate in Hz
    pub sample_rate: u32,
    /// Gain setting (Auto or Manual)
    pub gain: Gain,
    /// Enable bias tee power
    pub bias_tee: bool,
    /// Frequency correction in PPM
    pub ppm_correction: i32,
}

impl Default for RtlTcpConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: protocol::DEFAULT_PORT,
            center_freq: 100_000_000,
            sample_rate: 2_048_000,
            gain: Gain::Auto,
            bias_tee: false,
            ppm_correction: 0,
        }
    }
}

/// Send a command to the rtl_tcp server
fn send_command(stream: &mut TcpStream, cmd: u8, value: u32) -> Result<()> {
    let mut packet = [0u8; 5];
    packet[0] = cmd;
    packet[1..5].copy_from_slice(&value.to_be_bytes());
    stream.write_all(&packet)?;
    Ok(())
}

/// Parse the 12-byte handshake from server
fn parse_handshake(data: &[u8; 12]) -> Result<RtlTcpDongleInfo> {
    // Check magic
    if &data[0..4] != protocol::MAGIC {
        return Err(Error::device(format!(
            "Invalid rtl_tcp magic: expected 'RTL0', got {:?}",
            &data[0..4]
        )));
    }

    let tuner_type = u32::from_be_bytes([data[4], data[5], data[6], data[7]]);
    let gain_count = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);

    Ok(RtlTcpDongleInfo {
        tuner_type: TunerType::from_u32(tuner_type),
        gain_count,
    })
}

/// Configure the remote device with the given settings
fn configure_device(stream: &mut TcpStream, config: &RtlTcpConfig) -> Result<()> {
    // Set sample rate first (some servers require this before frequency)
    send_command(stream, protocol::CMD_SET_SAMPLE_RATE, config.sample_rate)?;

    // Set center frequency
    send_command(stream, protocol::CMD_SET_FREQUENCY, config.center_freq)?;

    // Set gain
    match config.gain {
        Gain::Auto => {
            send_command(stream, protocol::CMD_SET_GAIN_MODE, 0)?;
            send_command(stream, protocol::CMD_SET_AGC_MODE, 1)?;
        }
        Gain::Manual(gain_db) => {
            send_command(stream, protocol::CMD_SET_GAIN_MODE, 1)?;
            send_command(stream, protocol::CMD_SET_AGC_MODE, 0)?;
            // Convert dB to tenths of dB
            let gain_tenths = (gain_db * 10.0) as u32;
            send_command(stream, protocol::CMD_SET_GAIN, gain_tenths)?;
        }
    }

    // Set frequency correction
    if config.ppm_correction != 0 {
        send_command(
            stream,
            protocol::CMD_SET_FREQ_CORRECTION,
            config.ppm_correction as u32,
        )?;
    }

    // Set bias tee
    send_command(
        stream,
        protocol::CMD_SET_BIAS_TEE,
        if config.bias_tee { 1 } else { 0 },
    )?;

    Ok(())
}

/// Synchronous rtl_tcp I/Q sample reader
///
/// Connects to a remote rtl_tcp server and reads I/Q samples.
/// Implements `Iterator` to yield chunks of `Complex<f32>` samples.
pub struct RtlTcpReader {
    stream: TcpStream,
    dongle_info: RtlTcpDongleInfo,
    buf: Vec<u8>,
}

impl RtlTcpReader {
    /// Create a new rtl_tcp reader and connect to the server
    ///
    /// This will:
    /// 1. Connect to the remote server
    /// 2. Read and validate the handshake
    /// 3. Configure the device with the provided settings
    pub fn new(config: &RtlTcpConfig) -> Result<Self> {
        let addr = format!("{}:{}", config.host, config.port);

        // Connect with timeout
        let stream = TcpStream::connect_timeout(
            &addr.parse().map_err(|e| Error::device(format!("Invalid address: {}", e)))?,
            Duration::from_secs(protocol::CONNECT_TIMEOUT_SECS),
        )?;

        // Set read timeout to prevent blocking forever
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;

        let mut reader = Self {
            stream,
            dongle_info: RtlTcpDongleInfo {
                tuner_type: TunerType::Unknown,
                gain_count: 0,
            },
            buf: vec![0u8; protocol::DEFAULT_BUF_SIZE],
        };

        // Read handshake
        let mut handshake = [0u8; 12];
        reader.stream.read_exact(&mut handshake)?;
        reader.dongle_info = parse_handshake(&handshake)?;

        // Configure device
        configure_device(&mut reader.stream, config)?;

        Ok(reader)
    }

    /// Get information about the connected dongle
    pub fn dongle_info(&self) -> &RtlTcpDongleInfo {
        &self.dongle_info
    }

    /// Send a command to change the center frequency
    pub fn set_frequency(&mut self, freq_hz: u32) -> Result<()> {
        send_command(&mut self.stream, protocol::CMD_SET_FREQUENCY, freq_hz)
    }

    /// Send a command to change the sample rate
    pub fn set_sample_rate(&mut self, rate_hz: u32) -> Result<()> {
        send_command(&mut self.stream, protocol::CMD_SET_SAMPLE_RATE, rate_hz)
    }

    /// Send a command to change the gain
    pub fn set_gain(&mut self, gain: Gain) -> Result<()> {
        match gain {
            Gain::Auto => {
                send_command(&mut self.stream, protocol::CMD_SET_GAIN_MODE, 0)?;
                send_command(&mut self.stream, protocol::CMD_SET_AGC_MODE, 1)?;
            }
            Gain::Manual(gain_db) => {
                send_command(&mut self.stream, protocol::CMD_SET_GAIN_MODE, 1)?;
                send_command(&mut self.stream, protocol::CMD_SET_AGC_MODE, 0)?;
                let gain_tenths = (gain_db * 10.0) as u32;
                send_command(&mut self.stream, protocol::CMD_SET_GAIN, gain_tenths)?;
            }
        }
        Ok(())
    }
}

impl Iterator for RtlTcpReader {
    type Item = Result<Vec<Complex<f32>>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.stream.read(&mut self.buf) {
            Ok(0) => None, // Connection closed
            Ok(bytes_read) => {
                // Convert Cu8 bytes to Complex<f32>
                let samples = crate::convert_bytes_to_complex(
                    crate::IqFormat::Cu8,
                    &self.buf[..bytes_read],
                );
                Some(Ok(samples))
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                {
                    // Timeout - return empty but continue
                    Some(Ok(Vec::new()))
                } else {
                    Some(Err(e.into()))
                }
            }
        }
    }
}

/// Asynchronous rtl_tcp I/Q sample reader
///
/// Uses a background thread to handle the blocking TCP connection,
/// communicating with the async runtime via channels. This follows
/// the same pattern used by `AsyncRtlSdrReader`.
pub struct AsyncRtlTcpReader {
    rx: tokio::sync::mpsc::Receiver<Result<Vec<Complex<f32>>>>,
    command_tx: std::sync::mpsc::Sender<RtlTcpCommand>,
    _handle: std::thread::JoinHandle<()>,
    dongle_info: RtlTcpDongleInfo,
}

/// Commands that can be sent to the background reader thread
#[derive(Debug)]
pub enum RtlTcpCommand {
    /// Set center frequency in Hz
    SetFrequency(u32),
    /// Set sample rate in Hz
    SetSampleRate(u32),
    /// Set gain
    SetGain(Gain),
    /// Stop the reader
    Stop,
}

impl AsyncRtlTcpReader {
    /// Create a new async rtl_tcp reader
    ///
    /// Spawns a background thread to handle the blocking TCP I/O.
    pub fn new(config: &RtlTcpConfig) -> Result<Self> {
        let (tx, rx) = tokio::sync::mpsc::channel(32);
        let (command_tx, command_rx) = std::sync::mpsc::channel();
        let (init_tx, init_rx) = std::sync::mpsc::channel();

        let config = config.clone();

        let handle = std::thread::spawn(move || {
            Self::reader_thread(config, tx, command_rx, init_tx);
        });

        // Wait for initialization result
        let dongle_info = init_rx
            .recv()
            .map_err(|_| Error::device("Reader thread failed to initialize"))?
            .map_err(|e| Error::device(format!("Connection failed: {}", e)))?;

        Ok(Self {
            rx,
            command_tx,
            _handle: handle,
            dongle_info,
        })
    }

    fn reader_thread(
        config: RtlTcpConfig,
        tx: tokio::sync::mpsc::Sender<Result<Vec<Complex<f32>>>>,
        command_rx: std::sync::mpsc::Receiver<RtlTcpCommand>,
        init_tx: std::sync::mpsc::Sender<Result<RtlTcpDongleInfo>>,
    ) {
        // Connect and initialize
        let mut reader = match RtlTcpReader::new(&config) {
            Ok(r) => {
                let _ = init_tx.send(Ok(r.dongle_info.clone()));
                r
            }
            Err(e) => {
                let _ = init_tx.send(Err(e));
                return;
            }
        };

        // Set non-blocking for command polling
        let _ = reader
            .stream
            .set_read_timeout(Some(Duration::from_millis(100)));

        loop {
            // Check for commands (non-blocking)
            match command_rx.try_recv() {
                Ok(RtlTcpCommand::SetFrequency(freq)) => {
                    let _ = reader.set_frequency(freq);
                }
                Ok(RtlTcpCommand::SetSampleRate(rate)) => {
                    let _ = reader.set_sample_rate(rate);
                }
                Ok(RtlTcpCommand::SetGain(gain)) => {
                    let _ = reader.set_gain(gain);
                }
                Ok(RtlTcpCommand::Stop) => break,
                Err(std::sync::mpsc::TryRecvError::Empty) => {}
                Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
            }

            // Read I/Q data
            match reader.stream.read(&mut reader.buf) {
                Ok(0) => break, // Connection closed
                Ok(bytes_read) => {
                    let samples = crate::convert_bytes_to_complex(
                        crate::IqFormat::Cu8,
                        &reader.buf[..bytes_read],
                    );
                    if tx.blocking_send(Ok(samples)).is_err() {
                        break; // Receiver dropped
                    }
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    // Timeout is fine, just continue
                }
                Err(e) => {
                    let _ = tx.blocking_send(Err(e.into()));
                    break;
                }
            }
        }
    }

    /// Get information about the connected dongle
    pub fn dongle_info(&self) -> &RtlTcpDongleInfo {
        &self.dongle_info
    }

    /// Send a command to change the center frequency
    pub fn set_frequency(&self, freq_hz: u32) -> Result<()> {
        self.command_tx
            .send(RtlTcpCommand::SetFrequency(freq_hz))
            .map_err(|_| Error::device("Reader thread disconnected"))
    }

    /// Send a command to change the sample rate
    pub fn set_sample_rate(&self, rate_hz: u32) -> Result<()> {
        self.command_tx
            .send(RtlTcpCommand::SetSampleRate(rate_hz))
            .map_err(|_| Error::device("Reader thread disconnected"))
    }

    /// Send a command to change the gain
    pub fn set_gain(&self, gain: Gain) -> Result<()> {
        self.command_tx
            .send(RtlTcpCommand::SetGain(gain))
            .map_err(|_| Error::device("Reader thread disconnected"))
    }
}

impl Drop for AsyncRtlTcpReader {
    fn drop(&mut self) {
        let _ = self.command_tx.send(RtlTcpCommand::Stop);
    }
}

impl Stream for AsyncRtlTcpReader {
    type Item = Result<Vec<Complex<f32>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuner_type_parsing() {
        assert_eq!(TunerType::from_u32(0), TunerType::Unknown);
        assert_eq!(TunerType::from_u32(1), TunerType::E4000);
        assert_eq!(TunerType::from_u32(5), TunerType::R820T);
        assert_eq!(TunerType::from_u32(6), TunerType::R828D);
        assert_eq!(TunerType::from_u32(99), TunerType::Unknown);
    }

    #[test]
    fn test_handshake_parsing() {
        // Valid handshake: RTL0 + tuner_type=5 (R820T) + gain_count=29
        let handshake: [u8; 12] = [
            b'R', b'T', b'L', b'0', // Magic
            0, 0, 0, 5, // tuner_type = 5 (R820T)
            0, 0, 0, 29, // gain_count = 29
        ];

        let info = parse_handshake(&handshake).unwrap();
        assert_eq!(info.tuner_type, TunerType::R820T);
        assert_eq!(info.gain_count, 29);
    }

    #[test]
    fn test_handshake_invalid_magic() {
        let handshake: [u8; 12] = [
            b'X', b'X', b'X', b'X', // Invalid magic
            0, 0, 0, 5, 0, 0, 0, 29,
        ];

        let result = parse_handshake(&handshake);
        assert!(result.is_err());
    }

    #[test]
    fn test_config_default() {
        let config = RtlTcpConfig::default();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 1234);
        assert_eq!(config.sample_rate, 2_048_000);
    }
}
