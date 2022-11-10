#[macro_use]
extern crate amplify;

mod daemon;
mod p2p;
mod persistence;
mod router;
mod rpc;

use bitcoin_hashes::sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error as StdError;

use cyphernet::addr::{LocalNode, PeerAddr};
use cyphernet::crypto::ed25519::{Curve25519, PrivateKey};
use netservices::peer;
use netservices::peer::Action;
use reactor::{Reactor, ReactorApi};

use crate::daemon::Microservices;
use crate::router::RouterConfig;

pub type ResourceId = sha256::Hash;
pub type PeerId = PeerAddr<Curve25519>;
pub type RouteMap = BTreeMap<PeerId, BTreeSet<ResourceId>>;

fn main() -> Result<(), Box<dyn StdError>> {
    let mut reactor = Reactor::<Microservices>::new()?;

    let config = RouterConfig {
        persistence_pool_size: 4,
    };
    let context = peer::Context {
        method: Action::Connect("127.0.0.1".parse().unwrap()),
        local_node: LocalNode::from(PrivateKey::test()),
    };
    reactor.start_actor(Microservices::Router, daemon::Context::Router(config))?;
    reactor.start_actor(Microservices::P2p, daemon::Context::R2p(context))?;
    reactor.join().unwrap();
    Ok(())
}
