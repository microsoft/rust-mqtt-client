#![allow(unused_variables)]

pub mod client;
pub mod packet;
pub mod token;
pub mod error;
pub mod topic;

// NOTE: Any dispatching or connection management would be supplementary components.
// I am in favor of providing them, but they are built on top of these core components and would be optional.
// These core components have been designed to enable the future use of such additional components,
// e.g. AckToken is designed to support future usage in complex dispatching scenarios.