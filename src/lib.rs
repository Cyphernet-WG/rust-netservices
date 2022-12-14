#[macro_use]
extern crate amplify;
extern crate log_crate as log;

#[cfg(feature = "re-actor")]
pub mod actors;

#[cfg(feature = "io-reactor")]
pub mod resources;

mod auth;
mod connection;
mod frame;
mod listener;
pub mod noise;
mod session;
pub mod socks5;
mod transcoders;
pub mod tunnel;

pub use auth::Authenticator;
pub use connection::{Address, NetConnection, Proxy};
pub use frame::{Frame, Marshaller};
pub use listener::NetListener;
#[cfg(feature = "io-reactor")]
pub use resources::{ListenerEvent, NetAccept, NetResource, SessionEvent};
pub use session::NetSession;
