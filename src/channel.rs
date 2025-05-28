//! In-memory SPSC channel with history access.
//!
//! Provides a single-producer single-consumer channel to communicate between threads. It has the
//! added feature of being able to access past messages. The past messages to keep for later access
//! can be configures via a [`HistoryPolicy`].
//!
//! # Examples
//!
//! ## Keeping all past messages in history
//!
//! ```
//! use logicsponge_core::channel::channel;
//!
//! let (tx, rx) = channel();
//!
//! std::thread::spawn(move || {
//!     tx.send(42);
//!     tx.send(43);
//! });
//!
//! assert_eq!(rx.recv(), Ok(42));
//! assert_eq!(rx.recv(), Ok(43));
//!
//! assert_eq!(rx.history(0), Ok(43));
//! assert_eq!(rx.history(1), Ok(42));
//! ```
//!
//! ## Keeping only the newest message in history
//!
//! ```
//! use logicsponge_core::channel::{forgetful, HistoryError};
//!
//! let (tx, rx) = forgetful();
//!
//! std::thread::spawn(move || {
//!     tx.send(42);
//!     tx.send(43);
//! });
//!
//! assert_eq!(rx.recv(), Ok(42));
//! assert_eq!(rx.recv(), Ok(43));
//!
//! assert_eq!(rx.history(0), Ok(43));
//! assert!(rx.history(1).is_err());
//! ```

use delegate::delegate;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::mpsc;

pub use mpsc::RecvError;
pub use mpsc::SendError;
pub use mpsc::TryRecvError;

/// A policy for which past messages to keep.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
#[non_exhaustive]
pub enum HistoryPolicy {
    /// Keep all received messages in the history.
    KeepAll,
    /// Keep only the last received message in the history.
    KeepNewest,
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum HistoryError {
    Empty,
}

pub struct Sender<T> {
    tx_cb: mpsc::Sender<T>,
}

impl<T> Sender<T> {
    delegate! {
        to self.tx_cb {
            pub fn send(&self, msg: T) -> Result<(), SendError<T>>;
        }
    }
}

enum History<T> {
    Full(VecDeque<T>),
    Newest(Option<T>),
}

pub struct Receiver<T> {
    rx_cb: mpsc::Receiver<T>,
    history: RefCell<History<T>>,
}

impl<T> Receiver<T> {
    pub fn try_recv(&self) -> Result<T, TryRecvError>
    where
        T: Clone,
    {
        let res = self.rx_cb.try_recv();
        if let Ok(ref msg) = res {
            self.update_history(msg.clone());
        }
        res
    }

    pub fn recv(&self) -> Result<T, RecvError>
    where
        T: Clone,
    {
        let res = self.rx_cb.recv();
        if let Ok(ref msg) = res {
            self.update_history(msg.clone());
        }
        res
    }

    pub fn history(&self, index: usize) -> Result<T, HistoryError>
    where
        T: Clone,
    {
        match &*self.history.borrow() {
            History::Full(queue) => {
                if let Some(queue_index) = queue.len().checked_sub(index + 1) {
                    if let Some(msg) = queue.get(queue_index) {
                        return Ok(msg.clone());
                    }
                }
            }
            History::Newest(Some(msg)) => {
                if index == 0 {
                    return Ok(msg.clone());
                }
            }
            _ => {}
        }

        Err(HistoryError::Empty)
    }

    fn update_history(&self, msg: T) {
        let mut hist = self.history.borrow_mut();
        match &mut *hist {
            History::Full(queue) => queue.push_back(msg),
            History::Newest(slot) => *slot = Some(msg),
        }
    }
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    channel_with_policy::<T>(HistoryPolicy::KeepAll)
}

pub fn forgetful<T>() -> (Sender<T>, Receiver<T>) {
    channel_with_policy::<T>(HistoryPolicy::KeepNewest)
}

/// Constructs a (`Sender`, `Receiver`) pair of a new channel.
pub fn channel_with_policy<T>(policy: HistoryPolicy) -> (Sender<T>, Receiver<T>) {
    let (tx_cb, rx_cb) = mpsc::channel::<T>();
    (
        Sender { tx_cb },
        Receiver {
            rx_cb,
            history: RefCell::new(match policy {
                HistoryPolicy::KeepAll => History::Full(VecDeque::new()),
                HistoryPolicy::KeepNewest => History::Newest(None),
            }),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new() {
        let (_tx, _rx) = channel::<i32>();
    }

    #[test]
    fn send() {
        let (tx, _rx) = channel();

        tx.send(42).unwrap();
    }

    #[test]
    fn try_recv_empty() {
        let (_tx, rx) = channel::<i32>();

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn try_recv_one_elem() {
        let (tx, rx) = channel();

        tx.send(42).unwrap();

        assert_eq!(rx.try_recv(), Ok(42));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn try_recv_two_elem() {
        let (tx, rx) = channel();

        tx.send(42).unwrap();
        tx.send(43).unwrap();

        assert_eq!(rx.try_recv(), Ok(42));
        assert_eq!(rx.try_recv(), Ok(43));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn try_recv_two_elem_interleaved() {
        let (tx, rx) = channel();

        tx.send(42).unwrap();

        assert_eq!(rx.try_recv(), Ok(42));
        assert!(rx.try_recv().is_err());

        tx.send(43).unwrap();

        assert_eq!(rx.try_recv(), Ok(43));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn recv_one_elem() {
        let (tx, rx) = channel();

        std::thread::spawn(move || {
            tx.send(42).unwrap();
        });

        assert_eq!(rx.recv(), Ok(42));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn recv_two_elem() {
        let (tx, rx) = channel();

        std::thread::spawn(move || {
            tx.send(42).unwrap();
            tx.send(43).unwrap();
        });

        assert_eq!(rx.recv(), Ok(42));
        assert_eq!(rx.recv(), Ok(43));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn history_empty() {
        let (_tx, rx) = channel::<i32>();

        assert_eq!(rx.history(0), Err(HistoryError::Empty));
        assert_eq!(rx.history(1), Err(HistoryError::Empty));
    }

    #[test]
    fn history_one_keepall() {
        let (tx, rx) = channel();

        tx.send(42).unwrap();

        assert_eq!(rx.recv(), Ok(42));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));

        assert_eq!(rx.history(0), Ok(42));
        assert_eq!(rx.history(1), Err(HistoryError::Empty));
    }

    #[test]
    fn history_two_keepall() {
        let (tx, rx) = channel();

        tx.send(42).unwrap();
        tx.send(43).unwrap();

        assert_eq!(rx.recv(), Ok(42));

        assert_eq!(rx.history(0), Ok(42));
        assert_eq!(rx.history(1), Err(HistoryError::Empty));
        assert_eq!(rx.history(2), Err(HistoryError::Empty));

        assert_eq!(rx.recv(), Ok(43));
        assert!(rx.try_recv().is_err());

        assert_eq!(rx.history(0), Ok(43));
        assert_eq!(rx.history(1), Ok(42));
        assert_eq!(rx.history(2), Err(HistoryError::Empty));
    }

    #[test]
    fn history_one_keepnewest() {
        let (tx, rx) = forgetful();

        tx.send(42).unwrap();

        assert_eq!(rx.recv(), Ok(42));
        assert!(rx.try_recv().is_err());

        assert_eq!(rx.history(0), Ok(42));
        assert_eq!(rx.history(1), Err(HistoryError::Empty));
    }

    #[test]
    fn history_two_keepnewest() {
        let (tx, rx) = forgetful();

        tx.send(42).unwrap();
        tx.send(43).unwrap();

        assert_eq!(rx.recv(), Ok(42));
        assert_eq!(rx.recv(), Ok(43));
        assert!(rx.try_recv().is_err());

        assert_eq!(rx.history(0), Ok(43));
        assert_eq!(rx.history(1), Err(HistoryError::Empty));
        assert_eq!(rx.history(2), Err(HistoryError::Empty));
    }

    #[test]
    fn history_two_keepnewest_interleaved() {
        let (tx, rx) = forgetful();

        tx.send(42).unwrap();
        tx.send(43).unwrap();

        assert_eq!(rx.recv(), Ok(42));

        assert_eq!(rx.history(0), Ok(42));
        assert_eq!(rx.history(1), Err(HistoryError::Empty));
        assert_eq!(rx.history(2), Err(HistoryError::Empty));

        assert_eq!(rx.recv(), Ok(43));
        assert!(rx.try_recv().is_err());

        assert_eq!(rx.history(0), Ok(43));
        assert_eq!(rx.history(1), Err(HistoryError::Empty));
        assert_eq!(rx.history(2), Err(HistoryError::Empty));
    }

    #[test]
    fn history_one_keepnewest_threads() {
        let (tx, rx) = forgetful();

        std::thread::spawn(move || {
            tx.send(42).unwrap();
        });

        assert_eq!(rx.recv(), Ok(42));
        assert!(rx.try_recv().is_err());

        assert_eq!(rx.history(0), Ok(42));
        assert_eq!(rx.history(1), Err(HistoryError::Empty));
    }

    #[test]
    fn history_two_keepnewest_threads() {
        let (tx, rx) = forgetful();

        std::thread::spawn(move || {
            tx.send(42).unwrap();
            tx.send(43).unwrap();
        });

        assert_eq!(rx.recv(), Ok(42));
        assert_eq!(rx.recv(), Ok(43));
        assert!(rx.try_recv().is_err());

        assert_eq!(rx.history(0), Ok(43));
        assert_eq!(rx.history(1), Err(HistoryError::Empty));
    }

    #[test]
    fn history_two_keepnewest_interleaved_threads() {
        let (tx, rx) = forgetful();

        std::thread::spawn(move || {
            tx.send(42).unwrap();
            tx.send(43).unwrap();
        });

        assert_eq!(rx.recv(), Ok(42));

        assert_eq!(rx.history(0), Ok(42));
        assert_eq!(rx.history(1), Err(HistoryError::Empty));

        assert_eq!(rx.recv(), Ok(43));
        assert!(rx.try_recv().is_err());

        assert_eq!(rx.history(0), Ok(43));
        assert_eq!(rx.history(1), Err(HistoryError::Empty));
    }
}
