use std::fmt::Debug;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::io::{AsRawFd, RawFd};
use std::time::Duration;
use std::{io, net};

use reactor::poller::IoEv;
use reactor::{Io, IoStatus, ReadNonblocking, Resource, WriteNonblocking};

use crate::{NetConnection, NetListener, NetSession};

/// Socket read buffer size.
const READ_BUFFER_SIZE: usize = u16::MAX as usize;
/// Maximum time to wait when reading from a socket.
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(6);
/// Maximum time to wait when writing to a socket.
const WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

#[derive(Debug)]
pub enum ListenerEvent<S: NetSession> {
    Accepted(S),
    Failure(io::Error),
}

#[derive(Debug)]
pub struct NetAccept<S: NetSession, L: NetListener<Stream = S::Connection> = TcpListener> {
    session_context: S::Context,
    listener: L,
}

impl<L: NetListener<Stream = S::Connection>, S: NetSession> AsRawFd for NetAccept<S, L> {
    fn as_raw_fd(&self) -> RawFd {
        self.listener.as_raw_fd()
    }
}

impl<L: NetListener<Stream = S::Connection>, S: NetSession> io::Write for NetAccept<S, L> {
    fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
        panic!("must not write to network listener")
    }

    fn flush(&mut self) -> io::Result<()> {
        panic!("must not write to network listener")
    }
}

impl<L: NetListener<Stream = S::Connection>, S: NetSession> WriteNonblocking for NetAccept<S, L> {
    fn set_write_nonblocking(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        panic!("must not write to network listener")
    }

    fn write_nonblocking(&mut self, buf: &[u8]) -> IoStatus {
        panic!("must not write to network listener")
    }

    fn flush_nonblocking(&mut self) -> IoStatus {
        panic!("must not write to network listener")
    }
}

impl<L: NetListener<Stream = S::Connection>, S: NetSession> NetAccept<S, L> {
    pub fn bind(addr: impl Into<net::SocketAddr>, session_context: S::Context) -> io::Result<Self> {
        let listener = L::bind(addr)?;
        listener.set_nonblocking(true)?;
        Ok(Self {
            session_context,
            listener,
        })
    }

    pub fn local_addr(&self) -> net::SocketAddr {
        self.listener.local_addr()
    }

    fn handle_accept(&mut self) -> io::Result<S> {
        let mut stream = self.listener.accept()?;
        stream.set_read_timeout(Some(READ_TIMEOUT))?;
        stream.set_write_timeout(Some(WRITE_TIMEOUT))?;
        stream.set_nonblocking(true)?;
        Ok(S::accept(stream, &self.session_context))
    }
}

impl<L: NetListener<Stream = S::Connection>, S: NetSession> Resource for NetAccept<S, L> {
    type Id = net::SocketAddr;
    type Event = ListenerEvent<S>;

    fn id(&self) -> Self::Id {
        self.listener.local_addr()
    }

    fn interests(&self) -> IoEv {
        IoEv::read_only()
    }

    fn handle_io(&mut self, io: Io) -> Option<Self::Event> {
        match io {
            Io::Read => None,
            Io::Write => Some(match self.handle_accept() {
                Err(err) => ListenerEvent::Failure(err),
                Ok(session) => ListenerEvent::Accepted(session),
            }),
        }
    }

    fn disconnect(self) -> io::Result<()> {
        // We disconnect by dropping the self
        Ok(())
    }
}

pub enum SessionEvent<S: NetSession> {
    // cloudhead: It would be useful for the service to know that a connection was established
    // with the peer, ie. a `Connected` event.
    Established(S::Id),
    Data(Vec<u8>),
    Terminated(io::Error),
}

#[derive(Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub enum TransportState {
    // cloudhead: There's a missing state here? ie. "connecting": the socket may be created,
    // but the connection isn't yet established.
    Handshake,
    Active,
    Terminated,
}

pub struct NetTransport<S: NetSession> {
    state: TransportState,
    session: S,
    inbound: bool,
    needs_flush: bool,
}

impl<S: NetSession> AsRawFd for NetTransport<S> {
    fn as_raw_fd(&self) -> RawFd {
        self.session.as_raw_fd()
    }
}

impl<S: NetSession> NetSession for NetTransport<S> {
    type Context = S::Context;
    type Connection = S::Connection;
    type Id = S::Id;
    type PeerAddr = S::PeerAddr;
    type TransitionAddr = S::TransitionAddr;

    fn accept(connection: Self::Connection, context: &Self::Context) -> Self {
        todo!()
    }

    fn connect(addr: Self::PeerAddr, context: &Self::Context) -> io::Result<Self> {
        todo!()
    }

    fn id(&self) -> Option<Self::Id> {
        todo!()
    }

    fn handshake_completed(&self) -> bool {
        todo!()
    }

    fn transient_addr(&self) -> Self::TransitionAddr {
        todo!()
    }

    fn peer_addr(&self) -> Option<Self::PeerAddr> {
        todo!()
    }

    fn local_addr(&self) -> <Self::Connection as NetConnection>::Addr {
        todo!()
    }

    fn read_timeout(&self) -> io::Result<Option<Duration>> {
        todo!()
    }

    fn write_timeout(&self) -> io::Result<Option<Duration>> {
        todo!()
    }

    fn set_read_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        todo!()
    }

    fn set_write_timeout(&mut self, dur: Option<Duration>) -> io::Result<()> {
        todo!()
    }

    fn set_nonblocking(&mut self, nonblocking: bool) -> io::Result<()> {
        todo!()
    }

    fn disconnect(self) -> io::Result<()> {
        todo!()
    }
}

impl<S: NetSession> NetTransport<S> {
    fn upgrade(mut session: S, inbound: bool) -> io::Result<Self> {
        session.set_read_timeout(Some(READ_TIMEOUT))?;
        session.set_write_timeout(Some(WRITE_TIMEOUT))?;
        session.set_nonblocking(true)?; // cloudhead: This is already set in `S::connect`,
                                        // perhaps read/write timeout should be set there as well,
                                        // then there's no need to "upgrade".
        Ok(Self {
            state: TransportState::Handshake,
            session,
            inbound,
            needs_flush: true,
        })
    }

    pub fn accept(session: S) -> io::Result<Self> {
        Self::upgrade(session, true)
    }

    pub fn connect(addr: S::PeerAddr, context: &S::Context) -> io::Result<Self> {
        let session = S::connect(addr, context)?;
        let mut me = Self::upgrade(session, true)?; // cloudhead: `true` is for inbound, but inbound
                                                    // is false?
        me.inbound = false;
        Ok(me)
    }

    pub fn is_inbound(&self) -> bool {
        self.inbound
    }

    pub fn is_outbound(&self) -> bool {
        !self.is_inbound()
    }

    pub fn state(&self) -> TransportState {
        self.state
    }

    pub fn local_addr(&self) -> <S::Connection as NetConnection>::Addr {
        self.session.local_addr()
    }

    pub fn remote_addr(&self) -> Option<S::PeerAddr> {
        self.session.peer_addr()
    }

    pub fn transient_addr(&self) -> S::TransitionAddr {
        self.session.transient_addr()
    }

    pub fn peer_id(&self) -> Option<S::Id> {
        self.session.id()
    }

    pub fn expect_peer_id(&self) -> S::Id {
        self.session.expect_id()
    }

    fn handle_writable(&mut self) -> Option<SessionEvent<S>> {
        debug_assert_ne!(
            self.state,
            TransportState::Terminated,
            "read on terminated transport"
        );
        self.needs_flush = false;
        // cloudhead: How do we know that the connection is established?
        match self.session.flush_nonblocking() {
            IoStatus::Success(_) | IoStatus::WouldBlock => None,
            IoStatus::Shutdown => Some(SessionEvent::Terminated(io::ErrorKind::WriteZero.into())),
            IoStatus::Err(err) => Some(SessionEvent::Terminated(err)),
        }
    }

    fn handle_readable(&mut self) -> Option<SessionEvent<S>> {
        debug_assert_ne!(
            self.state,
            TransportState::Terminated,
            "read on terminated transport"
        );

        // We need to save the state before doing the read below
        let was_established = self.state == TransportState::Handshake;
        let mut buffer = vec![0; READ_BUFFER_SIZE];

        // Nb. Since `poll`, which this reactor is based on, is *level-triggered*,
        // we will be notified again if there is still data to be read on the socket.
        // Hence, there is no use in putting this socket read in a loop, as the second
        // invocation would likely block.
        match self.session.read_nonblocking(&mut buffer) {
            IoStatus::Success(0) if !was_established => {
                if self.session.handshake_completed() {
                    self.state = TransportState::Active;
                    return Some(SessionEvent::Established(self.session.expect_id()));
                } else {
                    // Do nothing since we haven't established session yet
                    None
                }
            }

            IoStatus::Shutdown => {
                self.state = TransportState::Terminated;
                Some(SessionEvent::Terminated(io::ErrorKind::Interrupted.into()))
            }

            IoStatus::Success(len) => {
                debug_assert!(was_established);
                debug_assert_eq!(len, buffer.len());
                Some(SessionEvent::Data(buffer))
            }

            IoStatus::WouldBlock => Some(SessionEvent::Data(buffer)),

            IoStatus::Err(err) => {
                self.state = TransportState::Terminated;
                Some(SessionEvent::Terminated(err))
            }
        }
    }
}

impl<S: NetSession> Resource for NetTransport<S>
where
    S::TransitionAddr: Into<net::SocketAddr>,
{
    type Id = RawFd;
    type Event = SessionEvent<S>;

    fn id(&self) -> Self::Id {
        self.session.as_raw_fd()
    }

    fn interests(&self) -> IoEv {
        if self.needs_flush {
            IoEv::read_write()
        } else {
            IoEv::read_only()
        }
    }

    fn handle_io(&mut self, io: Io) -> Option<Self::Event> {
        match io {
            Io::Read => self.handle_readable(),
            Io::Write => self.handle_writable(),
        }
    }

    fn disconnect(self) -> io::Result<()> {
        self.session.disconnect()
    }
}

impl<S: NetSession> Read for NetTransport<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.session.read(buf)
    }
}
impl<S: NetSession> ReadNonblocking for NetTransport<S> {
    fn set_read_nonblocking(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.session.set_read_timeout(timeout)
    }
}

impl<S: NetSession> Write for NetTransport<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.session.write(&buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.session.flush()
    }
}
impl<S: NetSession> WriteNonblocking for NetTransport<S> {
    fn set_write_nonblocking(&mut self, timeout: Option<Duration>) -> io::Result<()> {
        self.session.set_write_nonblocking(timeout)
    }

    fn flush_nonblocking(&mut self) -> IoStatus {
        self.needs_flush = false;
        self.session.flush_nonblocking()
    }
}

mod split {
    use super::*;

    #[derive(Clone, Debug, Display)]
    #[display("{error}")]
    pub struct SplitIoError<T: SplitIo> {
        pub original: T,
        pub error: T::Err,
    }

    impl<T: SplitIo + Debug> std::error::Error for SplitIoError<T> {}

    pub trait SplitIo: Sized {
        type Read: Read + Sized;
        type Write: Write + Sized;
        type Err: std::error::Error;

        /// # Panics
        ///
        /// If the split operation is not possible
        fn split_io(self) -> Result<(Self::Read, Self::Write), SplitIoError<Self>>;
        fn from_split_io(read: Self::Read, write: Self::Write) -> Self;
    }

    pub struct NetReader<S: NetSession> {
        state: TransportState,
        session: <S as SplitIo>::Read,
        inbound: bool,
    }

    impl<S: NetSession> Read for NetReader<S> {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.session.read(buf)
        }
    }

    pub struct NetWriter<S: NetSession> {
        state: TransportState,
        session: <S as SplitIo>::Write,
        inbound: bool,
    }

    impl<S: NetSession> Write for NetWriter<S> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.session.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.session.flush()
        }
    }

    impl<S: NetSession> SplitIo for NetTransport<S> {
        type Read = NetReader<S>;
        type Write = NetWriter<S>;
        type Err = S::Err;

        fn split_io(mut self) -> Result<(Self::Read, Self::Write), SplitIoError<Self>> {
            let (r, w) = match self.session.split_io() {
                Err(err) => {
                    self.session = err.original;
                    return Err(SplitIoError {
                        original: self,
                        error: err.error,
                    });
                }
                Ok(s) => s,
            };
            let reader = NetReader {
                state: self.state,
                session: r,
                inbound: self.inbound,
            };
            let writer = NetWriter {
                state: self.state,
                session: w,
                inbound: self.inbound,
            };
            Ok((reader, writer))
        }

        fn from_split_io(read: Self::Read, write: Self::Write) -> Self {
            debug_assert_eq!(read.state, write.state);
            debug_assert_eq!(read.inbound, write.inbound);
            Self {
                state: read.state,
                inbound: read.inbound,
                session: S::from_split_io(read.session, write.session),
                needs_flush: true,
            }
        }
    }
}
pub use split::*;
