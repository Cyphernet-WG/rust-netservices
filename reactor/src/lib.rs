#[macro_use]
extern crate amplify;

#[cfg(feature = "epoll")]
pub mod epoll;
#[cfg(feature = "mio")]
pub mod mio;
#[cfg(feature = "polling")]
pub mod polling;
#[cfg(feature = "popol")]
pub mod popol;
mod timeout;

pub use timeout::TimeoutManager;

use std::any::Any;
use std::collections::HashMap;
use std::hash::Hash;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use std::{io, thread};

use crossbeam_channel as chan;

/// Information about generated I/O events from the event loop.
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct IoSrc<S> {
    pub source: S,
    pub io: IoEv,
}

/// Specific I/O events which were received.
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
pub struct IoEv {
    pub is_readable: bool,
    pub is_writable: bool,
}

/// Resource is an I/O item operated by the [`crate::Reactor`]. It should
/// encompass all application-specific business logic for working with I/O
/// data and can operate as a state machine, advancing on I/O events via
/// calls to [`Resource::update_from_io`], dispatched by the reactor runtime.
/// Resources should handle such things as handshake, encryption, data encoding,
/// etc. and may execute their business logic by calling the reactor via
/// [`Controller`] handler provided during the resource construction. In such a
/// way they may create new resources and register them with the reactor,
/// disconnect other resources or send a data to them in a non-blocking way.
/// If a resource needs to perform extensive or blocking operation it is advised
/// to use dedicated worker threads. While this can be handled by the resource
/// internally, if the worker thread pool is desired it can be talked to via
/// set of channels specified as [`Resource::OutputChannels`] associated type
/// and provided to the resource upon its construction.
pub trait Resource<R: Resource = Self> {
    type Id: Clone + Eq + Ord + Hash + Send;
    type Context: Send;
    type Cmd: Send;
    type Error;

    fn with(context: Self::Context, controller: Controller<R>) -> Result<Self, Self::Error>
    where
        Self: Sized;

    fn id(&self) -> Self::Id;

    /// Performs input and/or output operations basing on the flags provided.
    /// For instance, flushes write queue or reads the data and executes
    /// certain business logic on the data read.
    ///
    /// Advances the state of the resources basing on the results of the I/O.
    ///
    /// The errors returned by this method are forwarded to [`Self::handle_err`].
    fn io_ready(&mut self, io: IoEv) -> Result<(), Self::Error>;

    /// Called by the reactor [`Runtime`] whenever it receives a command for this
    /// resource through the [`Controller`] [`ReactorApi`].
    ///
    /// The errors returned by this method are forwarded to [`Self::handle_err`].
    fn handle_cmd(&mut self, cmd: Self::Cmd) -> Result<(), Self::Error>;

    /// The errors returned by this method are forwarded to [`Broker::handle_err`].
    fn handle_err(&mut self, err: Self::Error) -> Result<(), Self::Error>;
}

/// Implements specific way of managing multiple resources for a reactor.
/// Blocks on concurrent events from multiple resources.
pub trait IoManager<R: Resource>: Iterator<Item = IoSrc<R::Id>> + Send {
    /// Detects whether a resource under the given id is known to the manager.
    fn has_resource(&self, id: &R::Id) -> bool;

    /// Adds already operating/connected resource to the manager.
    ///
    /// # I/O
    ///
    /// Implementations must not block on the operation or generate any I/O
    /// events.
    fn register_resource(&mut self, resource: &R) -> Result<(), R::Error>;

    /// Removes resource from the manager without disconnecting it or generating
    /// any events. Stops resource monitoring and returns the resource itself
    /// (like connection or a TCP stream). May be used later to insert resource
    /// back to the manager with [`Self::register_resource`].
    ///
    /// # I/O
    ///
    /// Implementations must not block on the operation or generate any I/O
    /// events.
    fn unregister_resource(&mut self, id: &R::Id) -> Result<(), R::Error>;

    /// Reads events from all resources under this manager.
    ///
    /// # Returns
    ///
    /// Whether the function has timed out.
    ///
    /// # I/O
    ///
    /// Blocks on the read operation or until the timeout.
    fn io_events(&mut self, timeout: Option<Duration>) -> Result<bool, R::Error>;
}

pub trait Broker<R: Resource>: Send {
    fn handle_err(&mut self, err: R::Error);
}

/// Implementation of reactor pattern.
///
/// Reactor manages multiple resources of homogenous type `R` (resource can be a
/// TCP connections, file descriptors or any other blocking resource). It does
/// concurrent read of the I/O events from the resources using
/// [`ResourceMgr::read_events`] method, and dispatches events to a
/// [`Handler`] in synchronous demultiplexed way. Finally, it can be controlled
/// from any outside thread - or from the handler - by using [`ReactorApi`] and
/// [`Controllers`] constructed by [`Reactor::controller`]. This includes ability
/// to connect or disconnect resources or send them data.
///
/// Reactor manages internally a thread which runs the [`Runtime`] event loop.
pub struct Reactor<R: Resource> {
    #[allow(dead_code)]
    thread: JoinHandle<()>,
    control: chan::Sender<ControlEvent<R>>,
    shutdown: chan::Sender<()>,
}

impl<R: Resource> Reactor<R> {
    /// Constructs reactor and runs it in a thread, returning [`Self`] as a
    /// controller exposing the API ([`ReactorApi`]).
    pub fn with(
        io: impl IoManager<R> + 'static,
        broker: impl Broker<R> + 'static,
    ) -> io::Result<Self>
    where
        R: 'static,
    {
        let (shutdown_send, shutdown_recv) = chan::bounded(1);
        let (control_send, control_recv) = chan::unbounded();

        let control = control_send.clone();
        let thread = thread::spawn(move || {
            let runtime = Runtime::new(io, control_recv, control_send, shutdown_recv, broker);
            runtime.run()
        });

        Ok(Reactor {
            thread,
            control,
            shutdown: shutdown_send,
        })
    }

    /// Returns controller implementing [`ReactorApi`] for this reactor.
    pub fn controller(&self) -> Controller<R> {
        Controller {
            control: self.control.clone(),
        }
    }

    /// Joins reactor runtime thread
    pub fn join(self) -> thread::Result<()> {
        self.thread.join()
    }

    /// Shut downs the reactor
    pub fn shutdown(self) -> Result<(), InternalError> {
        self.shutdown
            .send(())
            .map_err(|_| InternalError::ShutdownChanelBroken)?;
        self.join()?;
        Ok(())
    }
}

/// API for controlling the [`Reactor`] by the reactor instance or through
/// multiple [`Controller`]s constructed by [`Reactor::controller`].
pub trait ReactorApi {
    /// Resource type managed by the reactor.
    type Resource: Resource;

    /// Connects new resource and adds it to the manager.
    fn connect(&mut self, addr: <Self::Resource as Resource>::Context)
        -> Result<(), InternalError>;

    /// Disconnects from a resource, providing a reason.
    fn disconnect(&mut self, id: <Self::Resource as Resource>::Id) -> Result<(), InternalError>;

    /// Set one-time timer which will call [`Handler::on_timer`] upon expiration.
    fn set_timer(&mut self) -> Result<(), InternalError>;

    /// Send data to the resource.
    fn send(
        &mut self,
        id: <Self::Resource as Resource>::Id,
        data: <Self::Resource as Resource>::Cmd,
    ) -> Result<(), InternalError>;
}

/// Instance of reactor controller which may be transferred between threads
#[derive(Clone)]
pub struct Controller<R: Resource> {
    control: chan::Sender<ControlEvent<R>>,
}

impl<R: Resource> ReactorApi for chan::Sender<ControlEvent<R>> {
    type Resource = R;

    fn connect(&mut self, addr: R::Context) -> Result<(), InternalError> {
        chan::Sender::send(self, ControlEvent::Connect(addr))?;
        Ok(())
    }

    fn disconnect(&mut self, id: R::Id) -> Result<(), InternalError> {
        chan::Sender::send(self, ControlEvent::Disconnect(id))?;
        Ok(())
    }

    fn set_timer(&mut self) -> Result<(), InternalError> {
        chan::Sender::send(self, ControlEvent::SetTimer())?;
        Ok(())
    }

    fn send(&mut self, id: R::Id, data: R::Cmd) -> Result<(), InternalError> {
        chan::Sender::send(self, ControlEvent::Send(id, data))?;
        Ok(())
    }
}

impl<R: Resource> ReactorApi for Controller<R> {
    type Resource = R;

    fn connect(&mut self, addr: R::Context) -> Result<(), InternalError> {
        self.control.connect(addr)
    }

    fn disconnect(&mut self, id: R::Id) -> Result<(), InternalError> {
        self.control.disconnect(id)
    }

    fn set_timer(&mut self) -> Result<(), InternalError> {
        self.control.set_timer()
    }

    fn send(&mut self, id: R::Id, data: R::Cmd) -> Result<(), InternalError> {
        ReactorApi::send(&mut self.control, id, data)
    }
}

impl<R: Resource> ReactorApi for Reactor<R> {
    type Resource = R;

    fn connect(&mut self, addr: R::Context) -> Result<(), InternalError> {
        self.control.connect(addr)
    }

    fn disconnect(&mut self, id: R::Id) -> Result<(), InternalError> {
        self.control.disconnect(id)
    }

    fn set_timer(&mut self) -> Result<(), InternalError> {
        self.control.set_timer()
    }

    fn send(&mut self, id: R::Id, data: R::Cmd) -> Result<(), InternalError> {
        ReactorApi::send(&mut self.control, id, data)
    }
}

/// Runtime represents the reactor event loop with its state handled in a
/// dedicated thread by the reactor. It is controlled by sending instructions
/// through a set of crossbeam channels. [`Reactor`] abstracts that control via
/// exposing high-level [`ReactorApi`] and [`Controller`] objects.
struct Runtime<R: Resource, IO: IoManager<R>, B: Broker<R>> {
    resources: HashMap<R::Id, R>,
    io: IO,
    broker: B,
    control_recv: chan::Receiver<ControlEvent<R>>,
    control_send: chan::Sender<ControlEvent<R>>,
    shutdown: chan::Receiver<()>,
    timeouts: TimeoutManager<()>,
}

impl<R: Resource, IO: IoManager<R>, B: Broker<R>> Runtime<R, IO, B> {
    fn new(
        io: IO,
        control_recv: chan::Receiver<ControlEvent<R>>,
        control_send: chan::Sender<ControlEvent<R>>,
        shutdown: chan::Receiver<()>,
        broker: B,
    ) -> Self {
        Runtime {
            io,
            resources: empty!(),
            control_recv,
            control_send,
            shutdown,
            broker,
            timeouts: TimeoutManager::new(Duration::from_secs(0)),
        }
    }

    fn run(mut self) -> ! {
        loop {
            let now = Instant::now();
            if let Err(err) = self.io.io_events(self.timeouts.next(now)) {
                self.broker.handle_err(err);
            }
            for ev in &mut self.io {
                let res = self
                    .resources
                    .get_mut(&ev.source)
                    .expect("resource management inconsistency");
                res.io_ready(ev.io)
                    .or_else(|err| res.handle_err(err))
                    .unwrap_or_else(|err| self.broker.handle_err(err));
            }
            // TODO: Should we process control events before dispatching input?
            self.process_control();
            self.process_shutdown();
        }
    }

    fn process_control(&mut self) {
        loop {
            match self.control_recv.try_recv() {
                Err(chan::TryRecvError::Disconnected) => {
                    panic!("reactor shutdown channel was dropper")
                }
                Err(chan::TryRecvError::Empty) => break,
                Ok(event) => match event {
                    ControlEvent::Connect(context) => {
                        let controller = Controller {
                            control: self.control_send.clone(),
                        };
                        match R::with(context, controller) {
                            Err(err) => self.broker.handle_err(err),
                            Ok(mut resource) => {
                                self.io
                                    .register_resource(&resource)
                                    .or_else(|err| resource.handle_err(err))
                                    .unwrap_or_else(|err| self.broker.handle_err(err));
                                self.resources.insert(resource.id(), resource);
                            }
                        };
                        // TODO: Consider to error to the user if the resource was already present
                    }
                    ControlEvent::Disconnect(id) => {
                        self.io
                            .unregister_resource(&id)
                            .unwrap_or_else(|err| self.broker.handle_err(err));
                        self.resources.remove(&id);
                        // TODO: Don't we need to shutdown the resource?
                    }
                    ControlEvent::SetTimer() => {
                        // TODO: Add timeout manager
                    }
                    ControlEvent::Send(id, data) => {
                        if let Some(resource) = self.resources.get_mut(&id) {
                            resource
                                .handle_cmd(data)
                                .or_else(|err| resource.handle_err(err))
                                .unwrap_or_else(|err| self.broker.handle_err(err));
                        }
                    }
                },
            }
        }
    }

    fn process_shutdown(&mut self) {
        match self.shutdown.try_recv() {
            Err(chan::TryRecvError::Empty) => {
                // Nothing to do here
            }
            Ok(()) => {
                // TODO: Disconnect all resources
            }
            Err(chan::TryRecvError::Disconnected) => {
                panic!("reactor shutdown channel was dropper")
            }
        }
    }
}

#[derive(Debug, Display, Error, From)]
#[display(doc_comments)]
pub enum InternalError {
    /// shutdown channel in the reactor is broken
    ShutdownChanelBroken,

    /// control channel is broken; unable to send request
    ControlChannelBroken,

    /// error joining runtime
    #[from]
    ThreadError(Box<dyn Any + Send + 'static>),
}

impl<R: Resource> From<chan::SendError<ControlEvent<R>>> for InternalError {
    fn from(_: chan::SendError<ControlEvent<R>>) -> Self {
        InternalError::ControlChannelBroken
    }
}

/// Events send by [`Controller`] and [`ReactorApi`] to the [`Runtime`].
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Debug)]
enum ControlEvent<R: Resource> {
    /// Request reactor to connect to the resource with some context
    Connect(R::Context),

    /// Request reactor to disconnect from a resource
    Disconnect(R::Id),

    /// Ask reactor to wake up after certain interval
    SetTimer(),

    /// Request reactor to send the data to the resource
    Send(R::Id, R::Cmd),
}
