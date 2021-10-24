use log::*;
use std::fmt::Debug;
use std::pin::Pin;
use thiserror::Error;
use tokio::sync::broadcast;

use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

#[derive(Debug)]
pub enum EventStream<Item: Debug> {
    OpenForEvents(Item, BroadcastStream<Item>),
    ClosedForEvents(Item),
}

impl<Item: 'static + Debug + Clone + Send + Sync> EventStream<Item> {
    pub fn into_accumulated(self) -> Item {
        match self {
            EventStream::OpenForEvents(item, _) => item,
            EventStream::ClosedForEvents(item) => item,
        }
    }

    // FIXME: should not require map_fun
    pub fn into_stream<R, F>(self, map_fun: F) -> Pin<Box<dyn Stream<Item = R> + Send + Sync>>
    where
        R: 'static,
        F: Fn(Result<Item, BroadcastStreamRecvError>) -> R + Send + Sync + 'static,
    {
        match self {
            EventStream::OpenForEvents(item, stream) => Box::pin(
                tokio_stream::once(item)
                    .map(|item| Ok(item))
                    .chain(stream)
                    .map(map_fun),
            ),
            EventStream::ClosedForEvents(item) => {
                Box::pin(tokio_stream::once(item).map(|item| Ok(item)).map(map_fun))
            }
        }
    }
}

#[derive(Debug)]
pub struct EventStorage<Item: Debug> {
    accumulated: Item,
    sender: Option<broadcast::Sender<Item>>, // set to None when finished
}

#[derive(Debug, Error)]
pub enum AddEventError {
    #[error("Cannot add event after calling no_more_events")]
    AlreadyFinished,
}

impl<Item> EventStorage<Item>
where
    Item: Debug + Clone + Default + std::ops::Add<Output = Item> + Send + 'static,
{
    pub fn new(capacity: usize) -> Self {
        let (sender, _receiver) = broadcast::channel(capacity);
        EventStorage {
            accumulated: Default::default(),
            sender: Some(sender),
        }
    }

    pub fn add_event(&mut self, event: Item) -> Result<(), AddEventError> {
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

    pub fn get_event_stream(&self, client_id: String) -> EventStream<Item> {
        debug!("{} Subscribing", client_id);
        if let Some(sender) = &self.sender {
            EventStream::OpenForEvents(
                self.accumulated.clone(),
                BroadcastStream::new(sender.subscribe()),
            )
        } else {
            EventStream::ClosedForEvents(self.accumulated.clone())
        }
    }
}
