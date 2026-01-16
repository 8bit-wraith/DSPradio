# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Desperado** (`dsp_radio` crate) is a Rust library for reading I/Q samples from files, SDR devices, and streams. It provides a unified interface for both synchronous (`IqSource` + `Iterator`) and asynchronous (`IqAsyncSource` + `Stream`) I/Q data access, outputting normalized `Complex<f32>` values regardless of source format.

## Build & Development Commands

```bash
# Build (default - no hardware features)
cargo build

# Build with specific SDR support
cargo build --features rtlsdr      # RTL-SDR devices
cargo build --features soapy       # SoapySDR devices (requires libsoapysdr)
cargo build --features pluto       # Adalm-Pluto (requires libiio)

# Run all tests
cargo test --all-features

# Run a single test
cargo test test_name --all-features

# Clippy lints (required before commits)
cargo clippy --all-features

# Format check
cargo fmt --check

# Run examples
cargo run --example file_iter --features clap -- --help
cargo run --example file_stream --features clap -- --help
cargo run --example waterfall --features waterfall -- --help
cargo run --example rtlsdr_fm --features audio -- --help
```

## Architecture

### Core Types (src/lib.rs)

- **`IqSource`**: Synchronous enum wrapping all I/Q sources, implements `Iterator<Item=Result<Vec<Complex<f32>>>>`
- **`IqAsyncSource`**: Async counterpart implementing `futures::Stream`
- **`IqFormat`**: Enum for data formats (Cu8, Cs8, Cs16, Cf32)
- **`DeviceConfig`**: URL-parseable device configuration (`rtlsdr://0?freq=1090M&rate=2.4M`)
- **`Gain`**: Unified gain control (Auto/Manual)

### Source Modules

| Module | Feature Flag | Purpose |
|--------|--------------|---------|
| `iqread` | (always) | File/stdin/TCP I/Q readers with format conversion |
| `rtl_tcp` | (always) | Remote RTL-SDR via rtl_tcp protocol (network streaming) |
| `rtlsdr` | `rtlsdr` | RTL-SDR device support via pure Rust `rtl-sdr-rs` |
| `soapy` | `soapy` | SoapySDR bindings (HackRF, LimeSDR, etc.) |
| `pluto` | `pluto` | Adalm-Pluto via libiio bindings |

### DSP Module (src/dsp/)

Signal processing building blocks for FM demodulation pipelines:

- **`DspBlock` trait**: `fn process(&mut self, &[Complex<f32>]) -> Vec<Complex<f32>>`
- **`rotate`**: Complex rotation for frequency shifting
- **`decimator`**: Downsampling with anti-aliasing
- **`filters`**: FIR low-pass filter implementations
- **`fm`**: FM demodulation (PhaseExtractor, DeemphasisFilter)
- **`afc`**: Automatic Frequency Control
- **`rds`**: Radio Data System decoding
- **`resampler`** (feature `adaptive`): Audio sample rate conversion

DSP blocks are **not thread-safe** - each thread needs its own instances.

### Naming Conventions

- Sync types: `IqSource`, `IqRead`, `RtlSdrReader`
- Async types: `IqAsyncSource`, `IqAsyncRead`, `AsyncRtlSdrReader`

## I/Q Format Details

| Format | Bytes/Sample | Range | Common Use |
|--------|--------------|-------|------------|
| Cu8 | 2 | 0-255 (127.5 = zero) | RTL-SDR native |
| Cs8 | 2 | -128 to 127 | Some devices |
| Cs16 | 4 | -32768 to 32767 | HackRF, Pluto |
| Cf32 | 8 | normalized floats | Pre-processed |

## Device URL Syntax

```
rtlsdr://[device_index]?freq=<hz>&rate=<hz>&gain=<db|auto>&bias_tee=<bool>
rtl_tcp://host[:port]?freq=<hz>&rate=<hz>&gain=<db|auto>&ppm=<int>
soapy://<driver>?freq=<hz>&rate=<hz>&gain=<db|auto>
pluto://<uri>?freq=<hz>&rate=<hz>&gain=<db|auto>
plutoip://<ip>?...    # shorthand for pluto://ip:<ip>
```

SI suffixes supported: `k`, `M`, `G` (e.g., `freq=1090M`, `rate=2.4M`)

### rtl_tcp Example

Connect to a remote RTL-SDR running `rtl_tcp`:
```rust
// On remote host: rtl_tcp -a 0.0.0.0
let config = "rtl_tcp://192.168.1.100:1234?freq=1090M&rate=2.4M&gain=auto";
let source = IqSource::from_device_config(config.parse()?)?;
```

## Tests

Integration tests in `tests/`:
- `iq_conversion_test.rs`: Format conversion accuracy
- `iqread_test.rs`: File/stream reading

Run with: `cargo test --all-features`
