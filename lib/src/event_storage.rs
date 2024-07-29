use core::option;
use core::task::{Context, Poll};
use log::*;
use pin_project_lite::pin_project;
use std::fmt::Debug;
use std::pin::Pin;
use thiserror::Error;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Iter, Stream};

#[derive(Debug)]
pub struct EventSubscription<I: Debug> {
    accumulated: I,
    broadcast: Option<BroadcastStream<I>>,
}

impl<I: 'static + Debug + Clone + Send> EventSubscription<I> {
    pub fn into_accumulated(self) -> I {
        self.accumulated
    }

    pub fn into_stream(self) -> impl Stream<Item = Result<I, BroadcastStreamRecvError>> {
        match self {
            EventSubscription {
                accumulated: item,
                broadcast: None,
            } => EventStream::new_accumulated(item),
            EventSubscription {
                accumulated: item,
                broadcast: Some(broadcast),
            } => EventStream::new_broadcast(item, broadcast),
        }
    }
}

type FirstItem<I> = Iter<option::IntoIter<Result<I, BroadcastStreamRecvError>>>;

pin_project! {
    #[project = EventStreamProj]
    #[derive(Debug)]
    enum EventStream<I: Debug> {
        OpenForEvents{#[pin] first: FirstItem<I>, #[pin] broadcast: BroadcastStream<I>},
        ClosedForEvents{first: FirstItem<I>},
    }
}

impl<I: 'static + Debug + Clone> EventStream<I> {
    fn new_accumulated(accumulated: I) -> Self {
        EventStream::ClosedForEvents {
            first: tokio_stream::iter(Some(Ok(accumulated))),
        }
    }

    fn new_broadcast(accumulated: I, broadcast: BroadcastStream<I>) -> Self {
        EventStream::OpenForEvents {
            first: tokio_stream::iter(Some(Ok(accumulated))),
            broadcast,
        }
    }
}

impl<I: Debug + Clone + Send + 'static> Stream for EventStream<I> {
    type Item = Result<I, BroadcastStreamRecvError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.project() {
            EventStreamProj::ClosedForEvents { first } => Pin::new(first).poll_next(cx),
            EventStreamProj::OpenForEvents { first, broadcast } => {
                // first is created using once(), so it should yield the result on first try
                if let Poll::Ready(Some(v)) = first.poll_next(cx) {
                    return Poll::Ready(Some(v));
                }
                broadcast.poll_next(cx)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self {
            EventStream::ClosedForEvents { first } => first.size_hint(),
            EventStream::OpenForEvents { first, broadcast } => {
                merge_size_hints(first.size_hint(), broadcast.size_hint())
            }
        }
    }
}

#[derive(Debug)]
pub struct EventStorage<I: Debug> {
    accumulated: I,
    sender: Option<broadcast::Sender<I>>, // set to None when finished
}

#[derive(Debug, Error)]
pub enum AddEventError {
    #[error("Cannot add event after calling no_more_events")]
    AlreadyFinished,
}

impl<I> EventStorage<I>
where
    // TODO low replace Add with FnOnce
    I: Debug + Clone + Default + std::ops::Add<Output = I> + Send + 'static,
{
    pub fn new(capacity: usize) -> Self {
        let (sender, _receiver) = broadcast::channel(capacity);
        EventStorage {
            accumulated: Default::default(),
            sender: Some(sender),
        }
    }

    pub fn add_event(&mut self, event: I) -> Result<(), AddEventError> {
        // TODO low do not clone unless needed
        self.accumulated = std::mem::take(&mut self.accumulated) + event.clone();

        // Ignore possible sending errors when nobody is listening.
        let _ = self
            .sender
            .as_ref()
            .ok_or(AddEventError::AlreadyFinished)?
            .send(event);
        Ok(())
    }

    pub fn no_more_events(&mut self) {
        self.sender.take();
    }

    pub fn subscribe(&self) -> EventSubscription<I> {
        if let Some(sender) = &self.sender {
            EventSubscription {
                accumulated: self.accumulated.clone(),
                broadcast: Some(BroadcastStream::new(sender.subscribe())),
            }
        } else {
            EventSubscription {
                accumulated: self.accumulated.clone(),
                broadcast: None,
            }
        }
    }
}

// from tokio-stream 0.3.1 stream_ext.rs
fn merge_size_hints(
    (left_low, left_high): (usize, Option<usize>),
    (right_low, right_hign): (usize, Option<usize>),
) -> (usize, Option<usize>) {
    let low = left_low.saturating_add(right_low);
    let high = match (left_high, right_hign) {
        (Some(h1), Some(h2)) => h1.checked_add(h2),
        _ => None,
    };
    (low, high)
}
