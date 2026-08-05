//! Versioned canonical logging contracts and lifecycle invariants.
//!
//! This module defines the semantic event types used by the logging system.
//! `OutputEvent` remains a presentation adapter and is never persisted raw.

pub mod artifacts;
pub mod envelope;
pub mod events;
pub mod identifiers;
pub mod lifecycle;
pub mod proxy;
pub mod replay;
pub mod summaries;

#[cfg(test)]
mod tests;
