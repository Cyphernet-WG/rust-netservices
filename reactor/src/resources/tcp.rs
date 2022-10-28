use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::time::Duration;
use std::{io, net};
use streampipes::NetStream;

use crate::resources::FdResource;
use crate::{ConnDirection, InputEvent, OnDemand, Resource, ResourceAddr};

/// Maximum time to wait when reading from a socket.
const READ_TIMEOUT: Duration = Duration::from_secs(6);
/// Maximum time to wait when writing to a socket.
const WRITE_TIMEOUT: Duration = Duration::from_secs(3);
/// Size of the read buffer.
const READ_BUFFER_SIZE: usize = u16::MAX as usize;

/// Disconnect reason originating either from the network interface or provided
/// by the network protocol state machine in form of
/// [`ReactorDispatch::DisconnectPeer`] instruction.
#[derive(Debug, Clone)]
pub enum DisconnectReason {
    /// Error while dialing the remote. This error occurs before a connection is
    /// even established. Errors of this kind are usually not transient.
    DialError(Arc<io::Error>),

    /// Error with an underlying established connection. Sometimes, reconnecting
    /// after such an error is possible.
    ConnectionError(Arc<io::Error>),

    /// Peer was disconnected due to a request from the network protocol
    /// business logic.
    OnDemand,
}

impl OnDemand for DisconnectReason {
    fn on_demand() -> Self {
        Self::OnDemand
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TcpLocator<A> {
    Listener(net::SocketAddr),
    Connection(A),
}

impl<A: Send + Eq + Clone> ResourceAddr for TcpLocator<A> {}

impl<A> TcpLocator<A>
where
    net::SocketAddr: From<A>,
{
    pub fn socket_addr(&self) -> net::SocketAddr {
        match self {
            TcpLocator::Listener(addr) => *addr,
            TcpLocator::Connection(addr) => (*addr).into(),
        }
    }
}

// TODO: Make generic by the stream type allowing composition of streams
#[derive(Debug)]
pub enum TcpSocket<S: NetStream = net::TcpStream> {
    Listener(net::TcpListener),
    Stream(S),
}

impl<S: NetStream> TcpSocket<S>
where
    Self: Resource<Addr = TcpLocator<S::Addr>, Error = io::Error>,
{
    pub fn listen(addr: impl Into<net::SocketAddr>) -> io::Result<Self> {
        TcpSocket::connect(&TcpLocator::Listener(addr.into()))
    }

    pub fn dial(addr: impl Into<S::Addr>) -> io::Result<Self> {
        TcpSocket::connect(&TcpLocator::Connection(addr.into()))
    }
}

impl<S: NetStream> Resource for TcpSocket<S> {
    type Addr = TcpLocator<S::Addr>;
    type DisconnectReason = DisconnectReason;
    type Error = io::Error;

    fn addr(&self) -> Self::Addr {
        match self {
            TcpSocket::Listener(listener) => TcpLocator::Listener(
                listener
                    .local_addr()
                    .expect("TCP must always know local address"),
            ),
            TcpSocket::Stream(stream) => TcpLocator::Connection(
                stream
                    .peer_addr()
                    .expect("TCP stream always has remote address"),
            ),
        }
    }

    fn connect(addr: &Self::Addr) -> Result<Self, Self::Error> {
        match addr {
            TcpLocator::Listener(addr) => {
                let listener = net::TcpListener::bind(addr)?;
                listener.set_nonblocking(true)?;
                Ok(TcpSocket::Listener(listener))
            }
            TcpLocator::Connection(addr) => {
                use socket2::{Domain, Socket, Type};
                let socket_addr: net::SocketAddr = (*addr).into();
                let domain = if socket_addr.is_ipv4() {
                    Domain::IPV4
                } else {
                    Domain::IPV6
                };
                let sock = Socket::new(domain, Type::STREAM, None)?;

                sock.set_read_timeout(Some(READ_TIMEOUT))?;
                sock.set_write_timeout(Some(WRITE_TIMEOUT))?;
                sock.set_nonblocking(true)?;

                match sock.connect(&socket_addr.into()) {
                    Ok(()) => {}
                    Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
                    Err(e) if e.raw_os_error() == Some(libc::EALREADY) => {
                        return Err(io::Error::from(io::ErrorKind::AlreadyExists))
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                    Err(e) => return Err(e),
                }
                Ok(TcpSocket::Stream(sock.into()))
            }
        }
    }

    fn disconnect(&mut self) -> Result<(), Self::Error> {
        match self {
            TcpSocket::Listener(_) => {
                // Nothing to do here
            }
            TcpSocket::Stream(stream) => {
                stream.shutdown(net::Shutdown::Both)?;
            }
        }

        Ok(())
    }
}

impl<S: NetStream> FdResource for TcpSocket<S> {
    fn handle_readable(
        &mut self,
        events: &mut Vec<InputEvent<Self>>,
    ) -> Result<usize, Self::Error> {
        match self {
            TcpSocket::Listener(_) => {
                // We process the incoming connections in `fetch_writable`
                Ok(0)
            }
            TcpSocket::Stream(stream) => {
                let mut buffer = [0; READ_BUFFER_SIZE];
                let event = match stream.read(&mut buffer) {
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        // This shouldn't normally happen, since this function is only called
                        // when there's data on the socket. We leave it here in case external
                        // conditions change.
                        return Err(err);
                    }
                    Ok(0) | Err(_) => {
                        self.disconnect()?;
                        let reason = DisconnectReason::ConnectionError(Arc::new(io::Error::from(
                            io::ErrorKind::ConnectionReset,
                        )));
                        InputEvent::Disconnected(self.addr(), reason)
                    }
                    Ok(_) => InputEvent::Received(self.addr(), buffer.into()),
                };
                events.push(event);
                Ok(1)
            }
        }
    }

    fn handle_writable(
        &mut self,
        events: &mut Vec<InputEvent<Self>>,
    ) -> Result<usize, Self::Error> {
        let event = match self {
            TcpSocket::Listener(listener) => {
                let (conn, socket_addr) = listener.accept()?;
                conn.set_nonblocking(true)?;
                InputEvent::Connected {
                    remote_addr: TcpLocator::Connection(socket_addr),
                    local_addr: Some(TcpLocator::Connection(conn.local_addr()?)),
                    direction: ConnDirection::Inbound,
                }
            }
            TcpSocket::Stream(stream) => {
                if let Err(err) = stream.flush() {
                    self.disconnect()?;
                    InputEvent::Disconnected(
                        self.addr(),
                        DisconnectReason::ConnectionError(Arc::new(err)),
                    )
                } else {
                    return Ok(0);
                }
            }
        };
        events.push(event);
        Ok(1)
    }
}

impl<S: NetStream> AsRawFd for TcpSocket<S> {
    fn as_raw_fd(&self) -> RawFd {
        match self {
            TcpSocket::Listener(listener) => listener.as_raw_fd(),
            TcpSocket::Stream(stream) => stream.as_raw_fd(),
        }
    }
}

impl<S: NetStream> io::Read for TcpSocket<S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            TcpSocket::Listener(_) => Err(io::ErrorKind::NotConnected.into()),
            TcpSocket::Stream(stream) => stream.read(buf),
        }
    }
}

impl<S: NetStream> io::Write for TcpSocket<S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            TcpSocket::Listener(_) => Err(io::ErrorKind::NotConnected.into()),
            TcpSocket::Stream(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            TcpSocket::Listener(_) => Err(io::ErrorKind::NotConnected.into()),
            TcpSocket::Stream(stream) => stream.flush(),
        }
    }
}