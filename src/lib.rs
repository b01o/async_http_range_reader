//! This library provides the [`AsyncHttpRangeReader`] type.
//!
//! It allows streaming a file over HTTP while also allow random access. The type implements both
//! [`AsyncRead`] as well as [`AsyncSeek`]. This is supported through the use of range requests.
//! Each individual read will request a portion of the file using an HTTP range request.
//!
//! Requesting numerous small reads might turn out to be relatively slow because each reads needs to
//! perform an HTTP request. To alleviate this issue [`AsyncHttpRangeReader::prefetch`] is provided.
//! Using this method you can *prefect* a number of bytes which will be streamed in on the
//! background. If a read operation is reading from already (pre)fetched ranges it will stream from
//! the internal cache instead.
//!
//! Internally the [`AsyncHttpRangeReader`] stores a memory map which allows sparsely reading the
//! data into memory without actually requiring all memory for file to be resident in memory.
//!
//! The primary use-case for this library is to be able to sparsely stream a zip archive over HTTP
//! but its designed in a generic fashion.

mod error;
mod memory_map;
mod sparse_range;

use futures::{FutureExt, Stream, StreamExt};
use http_content_range::{ContentRange, ContentRangeBytes};
use itertools::Itertools;
use memory_map::MemoryMap;
use reqwest::header::HeaderMap;
use reqwest::{Response, Url};
use sparse_range::SparseRange;
use std::{
    io::{self, SeekFrom},
    ops::Range,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};
use tokio::{
    io::{AsyncRead, AsyncSeek, ReadBuf},
    sync::watch::Sender,
    sync::{Mutex, watch},
};
use tokio_stream::wrappers::WatchStream;
use tracing::*;
// use tokio_util::bytes;
use tokio_util::sync::PollSender;
use tracing::{Instrument, info, info_span};

pub use error::AsyncHttpRangeReaderError;

/// An `AsyncRangeReader` enables reading from a file over HTTP using range requests.
///
/// See the [`crate`] level documentation for more information.
///
/// The general entrypoint is [`AsyncHttpRangeReader::new`]. Depending on the
/// [`CheckSupportMethod`], this will either call [`AsyncHttpRangeReader::initial_tail_request`] or
/// [`AsyncHttpRangeReader::initial_head_request`] to send the initial request and then
/// [`AsyncHttpRangeReader::from_tail_response`] or [`AsyncHttpRangeReader::from_head_response`] to
/// initialize the async reader. If you want to apply a caching layer, you can send the initial head
/// (or tail) request yourself with your cache headers (e.g. through the
/// [http-cache-semantics](https://docs.rs/http-cache-semantics) crate):
///
/// ```rust
/// # use url::Url;
/// # use async_http_range_reader::{AsyncHttpRangeReader, AsyncHttpRangeReaderError};
/// # use reqwest::header::HeaderMap;
/// async fn get_reader_cached(
///     url: Url,
/// ) -> Result<Option<AsyncHttpRangeReader>, AsyncHttpRangeReaderError> {
///     let etag = "63c550e8-5ae";
///     let client = reqwest::Client::new();
///     let response = client
///         .head(url.clone())
///         .header(reqwest::header::IF_NONE_MATCH, etag)
///         .send()
///         .await?;
///     if response.status() == reqwest::StatusCode::NOT_MODIFIED {
///         Ok(None)
///     } else {
///         let reader = AsyncHttpRangeReader::from_head_response(client, response, url, HeaderMap::default()).await?;
///         Ok(Some(reader))
///     }
/// }
/// ```
#[derive(Debug)]
pub struct AsyncHttpRangeReader {
    inner: Mutex<Inner>,
    map: Arc<MemoryMap>,
    len: u64,
}

#[derive(Clone, Debug)]
struct StreamerState {
    requested_ranges: SparseRange,
    requested_time: usize,
    backward_memory_limit: u64,
    error: Option<AsyncHttpRangeReaderError>,
}

impl Default for StreamerState {
    fn default() -> Self {
        Self {
            requested_ranges: Default::default(),
            requested_time: 0,
            backward_memory_limit: 20 * 1024 * 1024,
            error: Default::default(),
        }
    }
}

#[derive(Debug)]
struct Inner {
    /// The current read position in the stream
    pos: u64,

    /// The range of bytes that have been requested for download
    requested_range: SparseRange,

    /// The range of bytes that have actually been downloaded to `data`.
    streamer_state: StreamerState,

    /// A channel receiver that holds the last downloaded range (or an error) from the background
    /// task.
    streamer_state_rx: WatchStream<StreamerState>,

    /// A channel sender to send range requests to the background task
    request_tx: tokio::sync::mpsc::Sender<Range<u64>>,

    /// An optional object to reserve a slot in the `request_tx` sender. When in the process of
    /// sending a requests this contains an actual value.
    poll_request_tx: Option<PollSender<Range<u64>>>,
}

/// For the initial request, we support either directly requesting N bytes from the end for file
/// or, if you the server doesn't support negative byte offsets, starting with a HEAD request
/// instead
pub enum CheckSupportMethod {
    /// Perform a range request with a negative byte range. This will return the N bytes from the
    /// *end* of the file as well as the file-size. This is especially useful to also immediately
    /// get some bytes from the end of the file.
    NegativeRangeRequest(u64),

    /// Perform a head request to get the length of the file and check if the server supports range
    /// requests.
    Head,
}

fn error_for_status(response: reqwest::Response) -> reqwest_middleware::Result<Response> {
    response
        .error_for_status()
        .map_err(reqwest_middleware::Error::Reqwest)
}

struct PositionResponse {
    start_position: u64,
    response: Response,
}

impl AsyncHttpRangeReader {
    /// Construct a new `AsyncHttpRangeReader`.
    pub async fn new(
        client: impl Into<reqwest_middleware::ClientWithMiddleware>,
        url: Url,
        check_method: CheckSupportMethod,
        extra_headers: HeaderMap,
    ) -> Result<(Self, HeaderMap), AsyncHttpRangeReaderError> {
        let client = client.into();
        match check_method {
            CheckSupportMethod::NegativeRangeRequest(initial_chunk_size) => {
                let response = Self::initial_tail_request(
                    client.clone(),
                    url.clone(),
                    initial_chunk_size,
                    HeaderMap::default(),
                )
                .await?;
                let response_headers = response.headers().clone();
                let self_ = Self::from_tail_response(client, response, url, extra_headers).await?;
                Ok((self_, response_headers))
            }
            CheckSupportMethod::Head => {
                let response =
                    Self::initial_head_request(client.clone(), url.clone(), HeaderMap::default())
                        .await?;
                let response_headers = response.headers().clone();
                let self_ = Self::from_head_response(client, response, url, extra_headers).await?;
                Ok((self_, response_headers))
            }
        }
    }

    /// Send an initial range request to determine if the remote accepts range
    /// requests. This will return a number of bytes from the end of the stream. Use the
    /// `initial_chunk_size` parameter to define how many bytes should be requested from the end.
    pub async fn initial_tail_request(
        client: impl Into<reqwest_middleware::ClientWithMiddleware>,
        url: reqwest::Url,
        initial_chunk_size: u64,
        extra_headers: HeaderMap,
    ) -> Result<Response, AsyncHttpRangeReaderError> {
        let client = client.into();
        let tail_response = client
            .get(url)
            .header(
                reqwest::header::RANGE,
                format!("bytes=-{initial_chunk_size}"),
            )
            .headers(extra_headers)
            .send()
            .await
            .and_then(error_for_status)
            .map_err(Arc::new)
            .map_err(AsyncHttpRangeReaderError::HttpError)?;
        Ok(tail_response)
    }

    /// Initialize the reader from [`AsyncHttpRangeReader::initial_tail_request`] (or a user
    /// provided response that also has a range of bytes from the end as body)
    pub async fn from_tail_response(
        client: impl Into<reqwest_middleware::ClientWithMiddleware>,
        tail_request_response: Response,
        url: Url,
        extra_headers: HeaderMap,
    ) -> Result<Self, AsyncHttpRangeReaderError> {
        let client = client.into();

        // Get the size of the file from this initial request
        let content_range_header = tail_request_response
            .headers()
            .get(reqwest::header::CONTENT_RANGE)
            .ok_or(AsyncHttpRangeReaderError::ContentRangeMissing)?
            .to_str()
            .map_err(|_err| AsyncHttpRangeReaderError::ContentRangeMissing)?;
        let content_range = ContentRange::parse(content_range_header).ok_or_else(|| {
            AsyncHttpRangeReaderError::ContentRangeParser(content_range_header.to_string())
        })?;
        let (start, finish, complete_length) = match content_range {
            ContentRange::Bytes(ContentRangeBytes { first_byte, last_byte, complete_length }) => {
                (first_byte, last_byte, complete_length)
            }
            _ => return Err(AsyncHttpRangeReaderError::HttpRangeRequestUnsupported),
        };

        let memory_store = Arc::new(MemoryMap::new(complete_length));
        let requested_range =
            SparseRange::from_range(complete_length - (finish - start)..complete_length);

        // adding more than 2 entries to the channel would block the sender. I assumed two would
        // suffice because I would want to 1) prefetch a certain range and 2) read stuff via the
        // AsyncRead implementation. Any extra would simply have to wait for one of these to
        // succeed. I eventually used 10 because who cares.
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(10);
        let (state_tx, state_rx) = watch::channel(StreamerState::default());
        tokio::spawn(run_streamer(
            client,
            url,
            extra_headers,
            Some((tail_request_response, start)),
            memory_store.clone(),
            state_tx,
            request_rx,
        ));

        // Configure the initial state of the streamer.
        let mut streamer_state = StreamerState::default();
        streamer_state
            .requested_ranges
            .update(complete_length - (finish - start)..complete_length);

        let reader = Self {
            len: complete_length,
            map: memory_store,
            inner: Mutex::new(Inner {
                pos: 0,
                requested_range,
                streamer_state,
                streamer_state_rx: WatchStream::new(state_rx),
                request_tx,
                poll_request_tx: None,
            }),
        };
        Ok(reader)
    }

    /// Send an initial range request to determine if the remote accepts range
    /// requests and get the content length
    pub async fn initial_head_request(
        client: impl Into<reqwest_middleware::ClientWithMiddleware>,
        url: reqwest::Url,
        extra_headers: HeaderMap,
    ) -> Result<Response, AsyncHttpRangeReaderError> {
        let client = client.into();

        // Perform a HEAD request to get the content-length.
        let head_response = client
            .head(url.clone())
            .headers(extra_headers)
            .send()
            .await
            .and_then(error_for_status)
            .map_err(Arc::new)
            .map_err(AsyncHttpRangeReaderError::HttpError)?;
        Ok(head_response)
    }

    /// Initialize the reader from [`AsyncHttpRangeReader::initial_head_request`] (or a user
    /// provided response the)
    pub async fn from_head_response(
        client: impl Into<reqwest_middleware::ClientWithMiddleware>,
        head_response: Response,
        url: Url,
        extra_headers: HeaderMap,
    ) -> Result<Self, AsyncHttpRangeReaderError> {
        let client = client.into();

        // Are range requests supported?
        if head_response
            .headers()
            .get(reqwest::header::ACCEPT_RANGES)
            .and_then(|h| h.to_str().ok())
            != Some("bytes")
        {
            return Err(AsyncHttpRangeReaderError::HttpRangeRequestUnsupported);
        }

        let content_length: u64 = head_response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .ok_or(AsyncHttpRangeReaderError::ContentLengthMissing)?
            .to_str()
            .map_err(|_err| AsyncHttpRangeReaderError::ContentLengthMissing)?
            .parse()
            .map_err(|_err| AsyncHttpRangeReaderError::ContentLengthMissing)?;

        let memory_map = Arc::new(MemoryMap::new(content_length));
        let requested_range = SparseRange::default();

        // adding more than 2 entries to the channel would block the sender. I assumed two would
        // suffice because I would want to 1) prefetch a certain range and 2) read stuff via the
        // AsyncRead implementation. Any extra would simply have to wait for one of these to
        // succeed. I eventually used 10 because who cares.
        let (request_tx, request_rx) = tokio::sync::mpsc::channel(10);
        let (state_tx, state_rx) = watch::channel(StreamerState::default());
        tokio::spawn(run_streamer(
            client,
            url,
            extra_headers,
            None,
            memory_map.clone(),
            state_tx,
            request_rx,
        ));

        // Configure the initial state of the streamer.
        let streamer_state = StreamerState::default();

        let reader = Self {
            len: content_length,
            map: memory_map,
            inner: Mutex::new(Inner {
                pos: 0,
                requested_range,
                streamer_state,
                streamer_state_rx: WatchStream::new(state_rx),
                request_tx,
                poll_request_tx: None,
            }),
        };
        Ok(reader)
    }

    /// Returns the ranges that this instance actually performed HTTP requests for.
    pub async fn requested_ranges(&self) -> Vec<Range<u64>> {
        let mut inner = self.inner.lock().await;
        if let Some(Some(new_state)) = inner.streamer_state_rx.next().now_or_never() {
            inner.streamer_state = new_state;
        }
        inner
            .streamer_state
            .requested_ranges
            .covered_ranges()
            .collect()
    }

    /// Prefetches a range of bytes from the remote. When specifying a large range this can
    /// drastically reduce the number of requests required to the server.
    pub async fn prefetch(&mut self, bytes: Range<u64>) {
        // Ensure the range is withing the file size and non-zero of length.
        let range = bytes.start..(bytes.end.min(self.map.len()));
        if range.start >= range.end {
            return;
        }

        // Check if the range has been requested or not.
        let inner = self.inner.get_mut();
        if let Some((new_range, _)) = inner.requested_range.cover(range.clone()) {
            let _ = inner.request_tx.send(range).await;
            inner.requested_range = new_range;
        }
    }

    /// Returns the length of the stream in bytes
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> u64 {
        self.len
    }

    pub async fn requested_time(&self) -> usize {
        self.inner.lock().await.streamer_state.requested_time
    }
}

/// A task that will download parts from the remote archive and "send" them to the frontend as they
/// become available.
#[tracing::instrument(name = "fetch_ranges", skip_all, fields(url))]
async fn run_streamer(
    client: reqwest_middleware::ClientWithMiddleware,
    url: Url,
    extra_headers: HeaderMap,
    initial_tail_response: Option<(Response, u64)>,
    memory_map: Arc<MemoryMap>,
    mut state_tx: Sender<StreamerState>,
    mut request_rx: tokio::sync::mpsc::Receiver<Range<u64>>,
) {
    let mut state = StreamerState::default();

    if let Some((response, response_start)) = initial_tail_response {
        // Add the initial range to the state
        state
            .requested_ranges
            .update(response_start..memory_map.len());
        state.requested_time += 1;

        let mut pr = PositionResponse { start_position: response_start, response };
        // Stream the initial data in memory
        if !stream_response(
            &mut pr,
            memory_map.len() - 1,
            &memory_map,
            &mut state_tx,
            &mut state,
        )
        .await
        {
            return;
        }
    }

    // save the response and its start position, reuse it later if possible
    let mut resp_opt: Option<PositionResponse> = None;

    // Listen for any new incoming requests
    'outer: loop {
        let range = match request_rx.recv().await {
            Some(range) => range,
            None => {
                break 'outer;
            }
        };

        // Determine the range that we need to cover
        let uncovered_ranges = match memory_map.inner.lock().unwrap().record.cover(range) {
            None => continue,
            Some((_, uncovered_ranges)) => uncovered_ranges,
        };

        // Download and stream each range.
        for range in uncovered_ranges {
            // Update the requested ranges
            debug!(?range, "Updating uncovered requested ranges");
            state
                .requested_ranges
                .update(*range.start()..*range.end() + 1);

            if !resp_opt
                .as_mut()
                .is_some_and(|PositionResponse { start_position, .. }| {
                    *start_position == *range.start()
                })
            {
                state.requested_time += 1;
                // Create a new request
                let range_string = format!("bytes={}-", range.start());
                info!(%range_string, "Requesting new range");
                let span = info_span!("fetch_range", range = range_string.as_str());
                let response = match client
                    .get(url.clone())
                    .header(reqwest::header::RANGE, range_string)
                    .headers(extra_headers.clone())
                    .send()
                    .instrument(span)
                    .await
                    .and_then(error_for_status)
                    .map_err(std::io::Error::other)
                {
                    Err(e) => {
                        state.error = Some(e.into());
                        let _ = state_tx.send(state);
                        break 'outer;
                    }
                    Ok(response) => response,
                };

                // If the server returns a successful, but non-206 response (e.g., 200), then it
                // doesn't support range requests (even if the `Accept-Ranges` header is set).
                // except for the case where the range is "bytes=0-". Server will return 200 OK
                if *range.start() != 0 && response.status() != reqwest::StatusCode::PARTIAL_CONTENT
                {
                    state.error = Some(AsyncHttpRangeReaderError::HttpRangeRequestUnsupported);
                    error!(
                        status = %response.status(),
                        "Server does not support range requests."
                    );
                    let _ = state_tx.send(state);
                    break 'outer;
                }

                resp_opt = Some(PositionResponse { start_position: *range.start(), response });
            }

            // SAFETY: We know that the response is not `None` because we just created it.
            let pr = unsafe { resp_opt.as_mut().unwrap_unchecked() };
            if !stream_response(pr, *range.end(), &memory_map, &mut state_tx, &mut state).await {
                break 'outer;
            }
        }
    }
}

#[tracing::instrument(
    name = "stream_response",
    level = "debug",
    skip_all,
    fields(position_response, target_end)
)]
/// Streams the data from the specified response to the memory map updating progress in between.
/// Reads from position `position_response.pos` until `target_end` is reached.
/// `pos` can be larger than `target_end` if the server returns a chunk has more data than needed.
/// Returns `true` if everything went fine, `false` if anything went wrong. The error state, if any,
/// is stored in `state_tx` so the "frontend" will consume it.
async fn stream_response(
    position_response: &mut PositionResponse,
    inclusive_target_end: u64,
    memory_map: &MemoryMap,
    state_tx: &mut Sender<StreamerState>,
    state: &mut StreamerState,
) -> bool {
    let pr = position_response;
    let protect_zone_pos = pr.start_position;

    while pr.start_position <= inclusive_target_end {
        let bytes = match pr.response.chunk().await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => break,
            Err(e) => {
                state.error = Some(e.into());
                let _ = state_tx.send(state.clone());
                return false;
            }
        };

        // Determine the range of these bytes in the complete file
        let byte_range = pr.start_position..pr.start_position + bytes.len() as u64;
        debug!(
            "Downloaded {} bytes from {} to {}",
            bytes.len(),
            byte_range.start,
            byte_range.end
        );

        memory_map.store_and_free(
            pr.start_position,
            bytes,
            protect_zone_pos,
            state.backward_memory_limit,
        );

        // Update the offset
        pr.start_position = byte_range.end;

        // Notify anyone that's listening that we have downloaded some extra data
        if state_tx.send(state.clone()).is_err() {
            // If we failed to set the state it means there is no receiver. In that case we should
            // just exit.
            return false;
        }
    }
    true
}

impl AsyncSeek for AsyncHttpRangeReader {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> io::Result<()> {
        let me = self.get_mut();
        let inner = me.inner.get_mut();

        inner.pos = match position {
            SeekFrom::Start(pos) => pos,
            SeekFrom::End(relative) => (me.map.len() as i64).saturating_add(relative) as u64,
            SeekFrom::Current(relative) => (inner.pos as i64).saturating_add(relative) as u64,
        };

        Ok(())
    }

    fn poll_complete(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<u64>> {
        let inner = self.inner.get_mut();
        Poll::Ready(Ok(inner.pos))
    }
}

impl AsyncRead for AsyncHttpRangeReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let me = self.get_mut();
        let inner = me.inner.get_mut();

        // If a previous error occurred we return that.
        if let Some(e) = inner.streamer_state.error.as_ref() {
            return Poll::Ready(Err(io::Error::other(e.clone())));
        }

        // Determine the range to be fetched
        let range = inner.pos..(inner.pos + buf.remaining() as u64).min(me.map.len());
        if range.start >= range.end {
            return Poll::Ready(Ok(()));
        }

        // Ensure we requested the required bytes
        while !inner.requested_range.is_covered(range.clone()) {
            // If there is an active range request wait for it to complete
            if let Some(mut poll) = inner.poll_request_tx.take() {
                match poll.poll_reserve(cx) {
                    Poll::Ready(_) => {
                        let _ = poll.send_item(range.clone());
                        inner.requested_range.update(range.clone());
                        break;
                    }
                    Poll::Pending => {
                        inner.poll_request_tx = Some(poll);
                        return Poll::Pending;
                    }
                }
            }

            // Request the range
            inner.poll_request_tx = Some(PollSender::new(inner.request_tx.clone()));
        }

        // If there is still a request poll open but there is no need for a request, abort it.
        if let Some(mut poll) = inner.poll_request_tx.take() {
            poll.abort_send();
        }

        loop {
            // Is the range already available?
            let is_covered = {
                me.map
                    .inner
                    .lock()
                    .unwrap()
                    .record
                    .is_covered(range.clone())
            };
            if is_covered {
                let len = (range.end - range.start) as usize;
                buf.initialize_unfilled_to(len).copy_from_slice(
                    me.map
                        .get(range.clone())
                        .unwrap_or_else(|| {
                            panic!("Failed to get bytes from memory map: {:?}", range)
                        })
                        .into_iter()
                        .collect_vec()
                        .as_slice(),
                );
                buf.advance(len);
                inner.pos += len as u64;
                return Poll::Ready(Ok(()));
            }

            // Otherwise wait for new data to come in
            match ready!(Pin::new(&mut inner.streamer_state_rx).poll_next(cx)) {
                None => unreachable!(),
                Some(state) => {
                    inner.requested_range = state.requested_ranges.clone();
                    inner.streamer_state = state;
                    if let Some(e) = inner.streamer_state.error.as_ref() {
                        return Poll::Ready(Err(io::Error::other(e.clone())));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod static_directory_server;

#[cfg(test)]
mod test {
    use super::*;
    use crate::static_directory_server::StaticDirectoryServer;
    use assert_matches::assert_matches;
    use async_zip::tokio::read::seek::ZipFileReader;
    use futures::AsyncReadExt;
    use reqwest::{Client, StatusCode};
    use rstest::*;
    use std::path::Path;
    use tokio::io::AsyncReadExt as _;

    #[rstest]
    #[case(CheckSupportMethod::Head)]
    #[case(CheckSupportMethod::NegativeRangeRequest(8192))]
    #[tokio::test]
    async fn async_range_reader_zip(#[case] check_method: CheckSupportMethod) {
        // Spawn a static file server
        let path = Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("test-data");
        let server = StaticDirectoryServer::new(&path)
            .await
            .expect("could not initialize server");

        // check that file is there and has the right size
        let filepath = path.join("andes-1.8.3-pyhd8ed1ab_0.conda");
        assert!(
            filepath.exists(),
            "The conda package is not there yet. Did you run `git lfs pull`?"
        );
        let file_size = std::fs::metadata(&filepath).unwrap().len();
        assert_eq!(
            file_size, 2_463_995,
            "The conda package is not there yet. Did you run `git lfs pull`?"
        );

        // Construct an AsyncRangeReader
        let (mut range, _) = AsyncHttpRangeReader::new(
            Client::new(),
            server.url().join("andes-1.8.3-pyhd8ed1ab_0.conda").unwrap(),
            check_method,
            HeaderMap::default(),
        )
        .await
        .expect("Could not download range - did you run `git lfs pull`?");

        // Make sure we have read the last couple of bytes
        range.prefetch(range.len() - 8192..range.len()).await;

        assert_eq!(range.len(), file_size);

        let mut reader = ZipFileReader::with_tokio(tokio::io::BufReader::with_capacity(0, range))
            .await
            .unwrap();

        assert_eq!(
            reader
                .file()
                .entries()
                .iter()
                .map(|e| e.filename().as_str().unwrap_or(""))
                .collect::<Vec<_>>(),
            vec![
                "metadata.json",
                "info-andes-1.8.3-pyhd8ed1ab_0.tar.zst",
                "pkg-andes-1.8.3-pyhd8ed1ab_0.tar.zst",
            ]
        );

        // Get the number of performed requests so far
        let request_ranges = reader
            .inner_mut()
            .get_mut()
            .get_mut()
            .requested_ranges()
            .await;
        let requested_time = reader
            .inner_mut()
            .get_mut()
            .get_mut()
            .requested_time()
            .await;
        assert_eq!(requested_time, 1);
        assert_eq!(
            request_ranges[0].end - request_ranges[0].start,
            8192,
            "first request should be the size of the initial chunk size"
        );
        assert_eq!(
            request_ranges[0].end, file_size,
            "first request should be at the end"
        );

        // Prefetch the data for the metadata.json file
        let entry = reader.file().entries().first().unwrap();
        let offset = entry.header_offset();
        // Get the size of the entry plus the header + size of the filename. We should also actually
        // include bytes for the extra fields but we don't have that information.
        let size = entry.compressed_size() + 30 + entry.filename().as_bytes().len() as u64;

        // The zip archive uses as BufReader which reads in chunks of 8192. To ensure we prefetch
        // enough data we round the size up to the nearest multiple of the buffer size.
        let buffer_size = 8192;
        let size = ((size + buffer_size - 1) / buffer_size) * buffer_size;

        // Fetch the bytes from the zip archive that contain the requested file.
        reader
            .inner_mut()
            .get_mut()
            .get_mut()
            .prefetch(offset..offset + size as u64)
            .await;

        // Read the contents of the metadata.json file
        let mut contents = String::new();
        reader
            .reader_with_entry(0)
            .await
            .unwrap()
            .read_to_string(&mut contents)
            .await
            .unwrap();

        // Get the number of performed requests
        let request_ranges = reader
            .inner_mut()
            .get_mut()
            .get_mut()
            .requested_ranges()
            .await;

        assert_eq!(contents, r#"{"conda_pkg_format_version": 2}"#);
        assert_eq!(request_ranges.len(), 2);

        assert_eq!(
            request_ranges[0],
            0..size,
            "expected only two range requests"
        );
    }

    #[rstest]
    #[case(CheckSupportMethod::Head)]
    #[case(CheckSupportMethod::NegativeRangeRequest(8192))]
    #[tokio::test]
    async fn async_range_reader(#[case] check_method: CheckSupportMethod) {
        // Spawn a static file server
        let path = Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("test-data");
        let server = StaticDirectoryServer::new(&path)
            .await
            .expect("could not initialize server");

        // Construct an AsyncRangeReader
        let (mut range, _) = AsyncHttpRangeReader::new(
            Client::new(),
            server.url().join("andes-1.8.3-pyhd8ed1ab_0.conda").unwrap(),
            check_method,
            HeaderMap::default(),
        )
        .await
        .expect("bla");

        // Also open a simple file reader
        let mut file = tokio::fs::File::open(path.join("andes-1.8.3-pyhd8ed1ab_0.conda"))
            .await
            .unwrap();

        // Read until the end and make sure that the contents matches
        let mut range_read = vec![0; 64 * 1024];
        let mut file_read = vec![0; 64 * 1024];
        loop {
            // Read with the async reader
            let range_read_bytes = range.read(&mut range_read).await.unwrap();

            // Read directly from the file
            let file_read_bytes = file
                .read_exact(&mut file_read[0..range_read_bytes])
                .await
                .unwrap();

            assert_eq!(range_read_bytes, file_read_bytes);
            assert_eq!(
                range_read[0..range_read_bytes],
                file_read[0..file_read_bytes]
            );

            if file_read_bytes == 0 && range_read_bytes == 0 {
                break;
            }
        }
    }

    #[tokio::test]
    async fn test_not_found() {
        let server = StaticDirectoryServer::new(Path::new(env!("CARGO_MANIFEST_DIR")))
            .await
            .expect("could not initialize server");
        let err = AsyncHttpRangeReader::new(
            Client::new(),
            server.url().join("not-found").unwrap(),
            CheckSupportMethod::Head,
            HeaderMap::default(),
        )
        .await
        .expect_err("expected an error");

        assert_matches!(
            err, AsyncHttpRangeReaderError::HttpError(err) if err.status() == Some(StatusCode::NOT_FOUND)
        );
    }
}
