extern crate futures;
extern crate bytes;
extern crate tokio_io;
extern crate tokio_timer;
extern crate io_dump;

use tokio_io::{AsyncRead, AsyncWrite};

use futures::{Future, Async, Poll};
use futures::task::{self, Task};

use tokio_timer::{Timer, Sleep};

use bytes::{Buf, BufMut};

use std::{cmp, fmt, io};
use std::collections::VecDeque;
use std::path::Path;
use std::time::Duration;
use std::sync::mpsc;

pub struct FixtureIo {
    state: Option<State>,
    actions: VecDeque<Action>,
    timer: Timer,
    read_wait: Option<Task>,
    drop_tx: mpsc::Sender<()>,
    drop_rx: Option<mpsc::Receiver<()>>,
}

#[derive(Debug)]
enum Action {
    Read(Vec<u8>),
    Write(Vec<u8>),
    Wait(Duration),
}

enum State {
    Reading(io::Cursor<Vec<u8>>),
    Writing(io::Cursor<Vec<u8>>),
    Waiting(Sleep),
}

impl FixtureIo {
    /// Returns a new `FixtureIo` that expects and returns nothing
    pub fn empty() -> FixtureIo {
        let (tx, rx) = mpsc::channel();

        FixtureIo {
            state: None,
            actions: VecDeque::new(),
            timer: Timer::default(),
            read_wait: None,
            drop_tx: tx,
            drop_rx: Some(rx),
        }
    }

    pub fn load<P: AsRef<Path>>(path: P) -> io::Result<FixtureIo> {
        use io_dump::{DumpRead, Direction};

        let mut ret = FixtureIo::empty();
        let mut last = Duration::from_millis(0);

        for block in try!(DumpRead::open(path)) {
            match block.direction() {
                Direction::Write => {
                    let data: Vec<u8> = block.data().into();
                    ret = ret.then_write(data);
                }
                Direction::Read => {
                    let wait = block.elapsed() - last;
                    let data: Vec<u8> = block.data().into();

                    ret = ret.then_wait(wait);
                    ret = ret.then_read(data);
                }
            }

            last = block.elapsed();
        }

        Ok(ret)
    }

    pub fn receiver(&mut self) -> mpsc::Receiver<()> {
        self.drop_rx.take().unwrap()
    }

    pub fn then_read<T: Into<Vec<u8>>>(mut self, data: T) -> Self {
        self.actions.push_back(Action::Read(data.into()));
        self
    }

    pub fn then_write<T: Into<Vec<u8>>>(mut self, data: T) -> Self {
        self.actions.push_back(Action::Write(data.into()));
        self
    }

    pub fn then_wait(mut self, duration: Duration) -> Self {
        self.actions.push_back(Action::Wait(duration));
        self
    }

    fn state(&mut self) -> Option<&mut State> {
        // If current action is complete, clear it
        if self.is_current_action_complete() {
            // Clear the state
            self.state = None;
        }

        if self.state.is_none() {
            // Get the next action and prepare it
            match self.actions.pop_front() {
                Some(Action::Read(data)) => {
                    let data = io::Cursor::new(data);
                    self.state = Some(State::Reading(data));
                }
                Some(Action::Write(data)) => {
                    let data = io::Cursor::new(data);
                    self.state = Some(State::Writing(data));
                }
                Some(Action::Wait(dur)) => {
                    let mut sleep = self.timer.sleep(dur);

                    // Poll, if ready, yield
                    if sleep.poll().unwrap().is_ready() {
                        task::current().notify();
                    }

                    self.state = Some(State::Waiting(sleep));
                }
                None => {}
            }
        }

        self.state.as_mut()
    }

    fn is_current_action_complete(&mut self) -> bool {
        match self.state {
            Some(State::Waiting(ref mut sleep)) => {
                sleep.poll().unwrap().is_ready()
            }
            Some(State::Reading(ref buf)) => {
                !buf.has_remaining()
            }
            Some(State::Writing(ref mut buf)) => {
                !buf.has_remaining()
            }
            _ => false,
        }
    }

    fn maybe_wakeup_reader(&mut self) {
        match self.state() {
            Some(&mut State::Reading(..)) | None => {
                if let Some(task) = self.read_wait.take() {
                    task.notify();
                }
            }
            _ => {}
        }
    }

    fn poll_read(&mut self) -> Async<()> {
        let ret = match self.state() {
            Some(ref state) if state.is_reading() => {
                Async::Ready(())
            }
            Some(_) => {
                Async::NotReady
            }
            None => {
                Async::Ready(())
            }
        };

        if !ret.is_ready() {
            self.read_wait = Some(task::current());
        }

        ret
    }
}

impl io::Read for FixtureIo {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        if !self.poll_read().is_ready() {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "would block"));
        }

        let n = match self.state() {
            Some(&mut State::Reading(ref mut buf)) => {
                let n = cmp::min(dst.len(), buf.remaining());
                io::Cursor::new(&mut dst[..n]).put(buf);
                n
            }
            None => {
                return Ok(0);
            }
            _ => {
                unreachable!();
            }
        };

        self.maybe_wakeup_reader();

        Ok(n)
    }
}

impl AsyncRead for FixtureIo {
}

impl io::Write for FixtureIo {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        let n = match self.state() {
            Some(&mut State::Writing(ref mut buf)) => {
                let pos = buf.position() as usize;
                let n;

                {
                    let buf = &buf.get_ref()[pos..];
                    n = cmp::min(buf.len(), src.len());

                    assert_eq!(&src[..n], &buf[..n]);
                }

                // Update the position
                buf.set_position(pos as u64 + n as u64);
                n
            }
            None => {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "broken pipe"));
            }
            _ => {
                return Err(io::Error::new(io::ErrorKind::WouldBlock, "would block"));
            }
        };

        self.maybe_wakeup_reader();

        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl AsyncWrite for FixtureIo {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        Ok(Async::Ready(()))
    }
}

impl Drop for FixtureIo {
    fn drop(&mut self) {
        let _ = self.drop_tx.send(());
    }
}

impl State {
    fn is_reading(&self) -> bool {
        match *self {
            State::Reading(..) => true,
            _ => false,
        }
    }
}

impl fmt::Debug for FixtureIo {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("FixtureIo")
            .field("state", &self.state)
            .field("actions", &self.actions)
            .finish()
    }
}

impl fmt::Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            State::Reading(ref buf) => {
                fmt.debug_struct("Reading")
                    .field("remaining", &buf.remaining())
                    .finish()
            }
            State::Writing(ref buf) => {
                fmt.debug_struct("Writing")
                    .field("remaining", &buf.remaining())
                    .finish()
            }
            State::Waiting(ref sleep) => {
                fmt.debug_struct("Waiting")
                    .field("remaining", &sleep.remaining())
                    .finish()
            }
        }
    }
}
