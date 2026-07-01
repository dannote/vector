use std::{
    num::NonZeroUsize,
    path::Path,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::future::BoxFuture;
use snafu::{ResultExt, Snafu};
use tower::Service;
use vector_lib::{
    EstimatedJsonEncodedSizeOf,
    codecs::encoding::{ArrowStreamSerializer, ArrowStreamSerializerConfig},
    event::{Event, EventFinalizers, EventStatus, Finalizable},
    request_metadata::{GroupedCountByteSize, MetaDescriptive, RequestMetadata},
    stream::DriverResponse,
};

use crate::{
    internal_events::EndpointBytesSent,
    sinks::prelude::{RequestMetadataBuilder, RetryLogic},
};

const DUCKDB_PROTOCOL: &str = "duckdb";

pub(super) fn default_database() -> String {
    "main".to_string()
}

#[derive(Debug, Snafu)]
pub enum DuckdbServiceError {
    #[snafu(display("DuckDB error: {source}"))]
    DuckDb { source: duckdb::Error },

    #[snafu(display("Arrow encoding error: {source}"))]
    ArrowEncoding {
        source: vector_lib::codecs::encoding::format::ArrowEncodingError,
    },

    #[snafu(display("Task join error: {source}"))]
    Join { source: tokio::task::JoinError },

    #[snafu(display("Connection mutex poisoned"))]
    MutexPoisoned,

    #[snafu(display("Payload should never be zero length"))]
    EmptyPayload,
}

#[derive(Clone)]
pub struct DuckdbRetryLogic;

impl RetryLogic for DuckdbRetryLogic {
    type Error = DuckdbServiceError;
    type Request = DuckdbRequest;
    type Response = DuckdbResponse;

    fn is_retriable_error(&self, error: &Self::Error) -> bool {
        // DuckDB writes are local and synchronous. Most failures are deterministic
        // data/schema/database errors, so do not retry by default.
        matches!(
            error,
            DuckdbServiceError::Join { .. } | DuckdbServiceError::MutexPoisoned
        )
    }
}

#[derive(Clone)]
pub struct DuckdbService {
    connection: Arc<Mutex<duckdb::Connection>>,
    database: String,
    table: String,
    endpoint: String,
    serializer: ArrowStreamSerializer,
}

impl DuckdbService {
    pub const fn new(
        connection: Arc<Mutex<duckdb::Connection>>,
        database: String,
        table: String,
        endpoint: String,
        serializer: ArrowStreamSerializer,
    ) -> Self {
        Self {
            connection,
            database,
            table,
            endpoint,
            serializer,
        }
    }
}

#[derive(Clone)]
pub struct DuckdbRequest {
    pub events: Vec<Event>,
    pub finalizers: EventFinalizers,
    pub metadata: RequestMetadata,
}

impl TryFrom<Vec<Event>> for DuckdbRequest {
    type Error = DuckdbServiceError;

    fn try_from(mut events: Vec<Event>) -> Result<Self, Self::Error> {
        let finalizers = events.take_finalizers();
        let metadata_builder = RequestMetadataBuilder::from_events(&events);
        let events_size = NonZeroUsize::new(events.estimated_json_encoded_size_of().get())
            .ok_or(DuckdbServiceError::EmptyPayload)?;
        let metadata = metadata_builder.with_request_size(events_size);
        Ok(Self {
            events,
            finalizers,
            metadata,
        })
    }
}

impl Finalizable for DuckdbRequest {
    fn take_finalizers(&mut self) -> EventFinalizers {
        self.finalizers.take_finalizers()
    }
}

impl MetaDescriptive for DuckdbRequest {
    fn get_metadata(&self) -> &RequestMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut RequestMetadata {
        &mut self.metadata
    }
}

pub struct DuckdbResponse {
    metadata: RequestMetadata,
}

impl DriverResponse for DuckdbResponse {
    fn event_status(&self) -> EventStatus {
        EventStatus::Delivered
    }

    fn events_sent(&self) -> &GroupedCountByteSize {
        self.metadata.events_estimated_json_encoded_byte_size()
    }

    fn bytes_sent(&self) -> Option<usize> {
        Some(self.metadata.request_encoded_size())
    }
}

impl Service<DuckdbRequest> for DuckdbService {
    type Response = DuckdbResponse;
    type Error = DuckdbServiceError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: DuckdbRequest) -> Self::Future {
        let connection = Arc::clone(&self.connection);
        let database = self.database.clone();
        let table = self.table.clone();
        let endpoint = self.endpoint.clone();
        let serializer = self.serializer.clone();

        let future = async move {
            let metadata = request.metadata;
            let record_batch = serializer
                .encode_to_record_batch(&request.events)
                .context(ArrowEncodingSnafu)?;

            tokio::task::spawn_blocking(move || {
                let mut conn = connection
                    .lock()
                    .map_err(|_| DuckdbServiceError::MutexPoisoned)?;
                let tx = conn
                    .transaction()
                    .map_err(|source| DuckdbServiceError::DuckDb { source })?;
                {
                    let mut appender = tx
                        .appender_to_db(&table, &database)
                        .map_err(|source| DuckdbServiceError::DuckDb { source })?;
                    appender
                        .append_record_batch(record_batch)
                        .map_err(|source| DuckdbServiceError::DuckDb { source })?;
                    appender
                        .flush()
                        .map_err(|source| DuckdbServiceError::DuckDb { source })?;
                }
                tx.commit()
                    .map_err(|source| DuckdbServiceError::DuckDb { source })
            })
            .await
            .context(JoinSnafu)??;

            emit!(EndpointBytesSent {
                byte_size: metadata.request_encoded_size(),
                protocol: DUCKDB_PROTOCOL,
                endpoint: &endpoint,
            });

            Ok(DuckdbResponse { metadata })
        };

        Box::pin(future)
    }
}

pub(super) fn build_serializer(
    schema: arrow::datatypes::Schema,
) -> Result<ArrowStreamSerializer, vector_lib::codecs::encoding::format::ArrowEncodingError> {
    ArrowStreamSerializer::new(ArrowStreamSerializerConfig::new(schema))
}

pub(super) fn open_connection(endpoint: &str) -> Result<duckdb::Connection, duckdb::Error> {
    match duckdb_path(endpoint) {
        DuckdbPath::Memory => duckdb::Connection::open_in_memory(),
        DuckdbPath::File(path) => duckdb::Connection::open(path),
    }
}

enum DuckdbPath<'a> {
    Memory,
    File(&'a Path),
}

fn duckdb_path(endpoint: &str) -> DuckdbPath<'_> {
    if endpoint == ":memory:" || endpoint == "duckdb:///:memory:" {
        return DuckdbPath::Memory;
    }

    if let Some(path) = endpoint.strip_prefix("duckdb://") {
        let path = if path.is_empty() { ":memory:" } else { path };
        if path == "/:memory:" || path == ":memory:" {
            DuckdbPath::Memory
        } else {
            DuckdbPath::File(Path::new(path))
        }
    } else {
        DuckdbPath::File(Path::new(endpoint))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_memory_endpoint() {
        assert!(matches!(duckdb_path(":memory:"), DuckdbPath::Memory));
        assert!(matches!(
            duckdb_path("duckdb:///:memory:"),
            DuckdbPath::Memory
        ));
    }

    #[test]
    fn parses_file_endpoint() {
        match duckdb_path("duckdb:///tmp/vector.duckdb") {
            DuckdbPath::File(path) => assert_eq!(path, Path::new("/tmp/vector.duckdb")),
            DuckdbPath::Memory => panic!("expected file path"),
        }

        match duckdb_path("relative.duckdb") {
            DuckdbPath::File(path) => assert_eq!(path, Path::new("relative.duckdb")),
            DuckdbPath::Memory => panic!("expected file path"),
        }
    }
}
