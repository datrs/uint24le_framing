//! Wrap bytes IO in length prefixed framing. Length is little endian 24 bit unsigned integer.
use crate::util::{stat_uint24_le, wrap_uint24_le};
use futures::{Sink, Stream, channel::mpsc};
use futures_lite::io::{AsyncRead, AsyncWrite};
use std::{
    collections::VecDeque,
    fmt::Debug,
    io::Result,
    pin::Pin,
    task::{Context, Poll},
};
use tracing::{error, info, instrument, trace, warn};

const BUF_SIZE: usize = 1024 * 64;
const _HEADER_LEN: usize = 3;

/// take a `AsyncWrite` of length prefixed messages and emit them as a Stream
pub struct Uint24LELengthPrefixedFraming<IO> {
    io: IO,
    /// Data from [`Self::io`]'s [`AsyncRead`] interface to be sent out via the [`Stream`] interface.
    to_stream: Vec<u8>,
    /// Data from the `Sink` interface to be written out to [`Self::io`]'s [`AsyncWrite`] interface.
    from_sink: VecDeque<Vec<u8>>,
    /// The index in [`Self::to_stream`] of the last byte that was sent to the [`Stream`].
    last_out_idx: usize,
    /// The index in [`Self::to_stream`] of the last byte that was read from [`Self::io`]'s
    /// [`AsyncRead`]
    last_data_idx: usize,
    /// Current step of a message being parsed
    step: Step,
}
impl<IO> Debug for Uint24LELengthPrefixedFraming<IO> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Framer")
            //.field("io", &self.io)
            .field("to_stream.len()", &self.to_stream.len())
            .field("from_sink", &self.from_sink.len())
            .field("last_out_idx", &self.last_out_idx)
            .field("last_data_idx", &self.last_data_idx)
            .field("step", &self.step)
            .finish()
    }
}
impl<IO> Uint24LELengthPrefixedFraming<IO>
where
    IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
{
    /// Build [`Uint24LELengthPrefixedFraming`] around an [`AsyncWrite`]/[`AsyncRead`] thing.
    pub fn new(io: IO) -> Self {
        Self {
            io,
            to_stream: vec![0u8; BUF_SIZE],
            from_sink: VecDeque::new(),
            last_out_idx: 0,
            last_data_idx: 0,
            step: Step::Header,
        }
    }

    /// Split off errors into a separate stream.
    ///
    /// Returns a tuple of:
    /// - A [`FramedWithErrors`] that implements `Stream<Item = Vec<u8>> + Sink<Vec<u8>>`
    /// - A stream of IO errors that occurred while reading
    ///
    /// When an error occurs on the underlying stream, it is sent to the error stream
    /// and the main stream terminates.
    pub fn split_off_errors(
        self,
    ) -> (
        FramedWithErrors<IO>,
        mpsc::UnboundedReceiver<std::io::Error>,
    ) {
        let (error_tx, error_rx) = mpsc::unbounded();
        (
            FramedWithErrors {
                inner: self,
                error_tx,
            },
            error_rx,
        )
    }
}

#[derive(Debug)]
enum Step {
    Header,
    Body { start: usize, end: u64 },
}

impl<IO> Stream for Uint24LELengthPrefixedFraming<IO>
where
    IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
{
    type Item = Result<Vec<u8>>;

    #[instrument(skip_all)]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Self {
            io,
            to_stream,
            last_out_idx,
            last_data_idx,
            step,
            ..
        } = self.get_mut();
        trace!(
            "Try to AsyncRead up to (buff_size[{}] - last_data_idx[{}]) = [{}]",
            to_stream.len(),
            *last_data_idx,
            to_stream.len() - *last_data_idx
        );
        let mut saw_eof = false;
        let n_bytes_read = match Pin::new(io).poll_read(cx, &mut to_stream[*last_data_idx..]) {
            Poll::Ready(Ok(0)) => {
                saw_eof = true;
                0
            }
            Poll::Ready(Ok(n)) => n,
            Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
            Poll::Pending => 0,
        };
        // TODO handle if to_stream is full
        trace!("adding #=[{n_bytes_read}] bytes to end=[{}]", last_data_idx);
        *last_data_idx += n_bytes_read;
        // grow buffer if it's full
        if *last_data_idx == to_stream.len() - 1 {
            warn!("Buffer full, double it's size");
            to_stream.extend(vec![0; to_stream.len()]);
        }

        if let Step::Header = step {
            trace!(step = ?*step, "enter");
            let cur_data = &to_stream[*last_out_idx..*last_data_idx];

            if let Some((header_len, body_len)) = stat_uint24_le(cur_data) {
                let cur_frame_start = *last_out_idx + header_len;
                let cur_frame_end = (cur_frame_start as u64) + body_len;
                *step = Step::Body {
                    start: cur_frame_start,
                    end: cur_frame_end,
                };
            } else {
                trace!("not enough bytes to read header");
            }
        }

        info!(step = ?*step, "enter");
        if let Step::Body { start, end } = step {
            let end = *end as usize;
            if end <= *last_data_idx {
                trace!(frame_size = end - *start, "Frame ready");
                let out = to_stream[*start..end].to_vec();
                *step = Step::Header;

                // remove bytes we're done with
                to_stream.rotate_left(end);
                *last_data_idx -= end;
                *last_out_idx = 0;

                return Poll::Ready(Some(Ok(out)));
            } else {
                trace!("Frame not ready start = {start}, end = {end}");
            }
        }
        if saw_eof {
            if *last_data_idx == *last_out_idx {
                // Clean EOF with no pending data
                return Poll::Ready(None);
            } else {
                // EOF with incomplete message
                return Poll::Ready(Some(Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed with incomplete message",
                ))));
            }
        }
        Poll::Pending
    }
}

impl<IO> Sink<Vec<u8>> for Uint24LELengthPrefixedFraming<IO>
where
    IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
{
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    #[instrument(skip_all)]
    fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> std::result::Result<(), Self::Error> {
        self.from_sink.push_back(wrap_uint24_le(&item));
        Ok(())
    }

    #[instrument(skip_all)]
    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let Self { from_sink, io, .. } = self.get_mut();
        loop {
            if let Some(msg) = from_sink.pop_front() {
                match Pin::new(&mut *io).poll_write(cx, &msg) {
                    Poll::Pending => {
                        from_sink.push_front(msg);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(n)) => {
                        if n != msg.len() {
                            from_sink.push_front(msg[n..].to_vec());
                            warn!("only wrote [{n} / {}] bytes of message", msg.len());
                        }
                        trace!("flushed whole message of N=[{n}] bytes");
                    }
                    Poll::Ready(Err(e)) => {
                        error!("Error flushing data");
                        return Poll::Ready(Err(e));
                    }
                }
            } else {
                trace!("No more messages to flush");
                return Pin::new(&mut *io).poll_flush(cx);
            }
        }
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        let Self { io, .. } = self.get_mut();
        Pin::new(&mut *io).poll_close(cx)
    }
}

/// A framed stream/sink that sends read errors to a separate channel.
///
/// Created by [`Uint24LELengthPrefixedFraming::split_off_errors`].
pub struct FramedWithErrors<IO> {
    inner: Uint24LELengthPrefixedFraming<IO>,
    error_tx: mpsc::UnboundedSender<std::io::Error>,
}

impl<IO> Debug for FramedWithErrors<IO> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FramedWithErrors")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<IO> Stream for FramedWithErrors<IO>
where
    IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
{
    type Item = Vec<u8>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(data))) => Poll::Ready(Some(data)),
            Poll::Ready(Some(Err(e))) => {
                let _ = this.error_tx.unbounded_send(e);
                Poll::Ready(None)
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<IO> Sink<Vec<u8>> for FramedWithErrors<IO>
where
    IO: AsyncWrite + AsyncRead + Send + Unpin + 'static,
{
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Vec<u8>) -> std::result::Result<(), Self::Error> {
        Pin::new(&mut self.get_mut().inner).start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().inner).poll_close(cx)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use crate::duplex::Duplex;

    use super::*;
    use futures::{FutureExt, SinkExt, StreamExt};
    use futures_lite::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use tokio::spawn;
    use tokio_util::compat::TokioAsyncReadCompatExt;

    pub(crate) fn duplex(
        channel_size: usize,
    ) -> (impl AsyncRead + AsyncWrite, impl AsyncRead + AsyncWrite) {
        let (left, right) = tokio::io::duplex(channel_size);
        (left.compat(), right.compat())
    }

    #[tokio::test]
    async fn duplex_works() -> Result<()> {
        let (mut left, mut right) = duplex(64);
        left.write_all(b"hello").await?;
        let mut b = vec![0; 5];
        right.read_exact(&mut b).await?;
        assert_eq!(b, b"hello");
        Ok(())
    }

    #[tokio::test]
    async fn input() -> Result<()> {
        let (left, mut right) = duplex(64);
        let mut lp = Uint24LELengthPrefixedFraming::new(left);
        let input = b"yelp";
        let msg = wrap_uint24_le(input);
        right.write_all(&msg).await?;
        let Some(Ok(rx)) = lp.next().await else {
            panic!()
        };
        assert_eq!(rx, input);
        Ok(())
    }
    #[tokio::test]
    async fn stream_many() -> Result<()> {
        let (left, mut right) = duplex(64);
        let mut lp = Uint24LELengthPrefixedFraming::new(left);
        let data: &[&[u8]] = &[b"yolo", b"squalor", b"idle", b"hello", b"stuff"];
        for d in data {
            let msg = wrap_uint24_le(d);
            right.write_all(&msg).await?;
        }
        for d in data {
            let Some(Ok(res)) = lp.next().await else {
                panic!();
            };
            assert_eq!(&res, d);
        }
        Ok(())
    }
    #[tokio::test]
    async fn sink_many() -> Result<()> {
        let (left, mut right) = duplex(64);
        let mut lp = Uint24LELengthPrefixedFraming::new(left);
        let data: &[&[u8]] = &[b"yolo", b"squalor", b"idle", b"hello", b"stuff"];
        for d in data {
            lp.send(d.to_vec()).await.unwrap();
        }

        let mut expected = vec![];
        data.iter().for_each(|d| expected.extend(wrap_uint24_le(d)));
        let mut result = vec![0; expected.len()];
        right.read_exact(&mut result).await?;
        assert_eq!(result, expected);
        Ok(())
    }

    #[tokio::test]
    async fn left_and_right() -> Result<()> {
        let (left, right) = duplex(64);

        let mut leftlp = Uint24LELengthPrefixedFraming::new(left);
        let mut rightlp = Uint24LELengthPrefixedFraming::new(right);

        let data: &[&[u8]] = &[b"yolo", b"squalor", b"idle", b"hello", b"stuff"];
        for d in data {
            rightlp.send(d.to_vec()).await.unwrap();
        }

        let mut result1 = vec![];
        for _ in data {
            result1.push(leftlp.next().await.unwrap().unwrap());
        }
        assert_eq!(result1, data);

        for d in data {
            leftlp.send(d.to_vec()).await.unwrap();
        }
        let mut result2 = vec![];
        for _ in data {
            result2.push(rightlp.next().await.unwrap().unwrap());
        }
        assert_eq!(result2, data);

        let mut r3 = vec![];
        let mut r4 = vec![];
        for d in data {
            rightlp.send(d.to_vec()).await.unwrap();
            leftlp.send(d.to_vec()).await.unwrap();
        }

        for _ in data {
            r3.push(rightlp.next().await.unwrap().unwrap());
            r4.push(leftlp.next().await.unwrap().unwrap());
        }
        assert_eq!(r3, data);
        assert_eq!(r4, data);

        Ok(())
    }
    #[tokio::test]
    async fn left_and_right_sluice() -> Result<()> {
        let (ar, bw) = sluice::pipe::pipe();
        let (br, aw) = sluice::pipe::pipe();
        let left = Duplex::new(ar, aw);
        let right = Duplex::new(br, bw);

        let mut leftlp = Uint24LELengthPrefixedFraming::new(left);
        let mut rightlp = Uint24LELengthPrefixedFraming::new(right);

        // NB sluice has a max "chunk" thing of 4
        // so we limit the data we're sending to 3 things
        let data: &[&[u8]] = &[b"yolo", b"squalor", b"idle"];
        // NB this sluice pipe
        //
        for d in data {
            rightlp.feed(d.to_vec()).await?;
        }
        let rflush = spawn(async move {
            rightlp.flush().await.unwrap();
            rightlp
        });

        let mut result1 = vec![];
        for _ in data {
            result1.push(leftlp.next().await.unwrap()?);
        }
        let mut rightlp = rflush.await?;

        assert_eq!(result1, data);

        for d in data {
            leftlp.feed(d.to_vec()).await?;
        }
        let lflush = spawn(async move {
            leftlp.flush().await.unwrap();
            leftlp
        });

        let mut result2 = vec![];
        for _ in data {
            result2.push(rightlp.next().await.unwrap()?);
        }
        let mut leftlp = lflush.await?;
        assert_eq!(result2, data);

        let mut r3 = vec![];
        let mut r4 = vec![];

        for d in data {
            rightlp.send(d.to_vec()).await?;
            leftlp.send(d.to_vec()).await?;
        }

        for _ in data {
            r3.push(rightlp.next().await.unwrap()?);
            r4.push(leftlp.next().await.unwrap()?);
        }

        assert_eq!(r3, data);
        assert_eq!(r4, data);

        Ok(())
    }

    #[tokio::test]
    async fn split_off_errors_stream() -> Result<()> {
        let (left, mut right) = duplex(64);
        let framed = Uint24LELengthPrefixedFraming::new(left);
        let (mut framed, _errors) = framed.split_off_errors();

        let data: &[&[u8]] = &[b"yolo", b"squalor", b"idle", b"hello", b"stuff"];
        for d in data {
            let msg = wrap_uint24_le(d);
            right.write_all(&msg).await?;
        }

        for d in data {
            let Some(res) = framed.next().await else {
                panic!("expected data");
            };
            assert_eq!(&res, d);
        }
        Ok(())
    }

    #[tokio::test]
    async fn split_off_errors_sink() -> Result<()> {
        let (left, mut right) = duplex(64);
        let framed = Uint24LELengthPrefixedFraming::new(left);
        let (mut framed, _errors) = framed.split_off_errors();

        let data: &[&[u8]] = &[b"yolo", b"squalor", b"idle", b"hello", b"stuff"];
        for d in data {
            framed.send(d.to_vec()).await.unwrap();
        }

        let mut expected = vec![];
        data.iter().for_each(|d| expected.extend(wrap_uint24_le(d)));
        let mut result = vec![0; expected.len()];
        right.read_exact(&mut result).await?;
        assert_eq!(result, expected);
        Ok(())
    }

    #[tokio::test]
    async fn split_off_errors_bidirectional() -> Result<()> {
        let (left, right) = duplex(64);

        let left_framed = Uint24LELengthPrefixedFraming::new(left);
        let (mut left_framed, _left_errors) = left_framed.split_off_errors();

        let right_framed = Uint24LELengthPrefixedFraming::new(right);
        let (mut right_framed, _right_errors) = right_framed.split_off_errors();

        let data: &[&[u8]] = &[b"yolo", b"squalor", b"idle", b"hello", b"stuff"];

        // Send from right to left
        for d in data {
            right_framed.send(d.to_vec()).await.unwrap();
        }

        let mut result1 = vec![];
        for _ in data {
            result1.push(left_framed.next().await.unwrap());
        }
        assert_eq!(result1, data);

        // Send from left to right
        for d in data {
            left_framed.send(d.to_vec()).await.unwrap();
        }

        let mut result2 = vec![];
        for _ in data {
            result2.push(right_framed.next().await.unwrap());
        }
        assert_eq!(result2, data);

        Ok(())
    }

    #[tokio::test]
    async fn split_off_errors_receives_error_on_close() -> Result<()> {
        let (left, right) = duplex(64);
        let framed = Uint24LELengthPrefixedFraming::new(left);
        let (mut framed, mut errors) = framed.split_off_errors();

        // Drop the other end to cause an error/EOF
        drop(right);

        // Stream should return None (EOF)
        let result = framed.next().await;
        assert!(result.is_none());

        // Note: EOF may not produce an error, just None
        // Check if there's an error (implementation dependent)
        let _maybe_error = errors.next().now_or_never();

        Ok(())
    }

    #[tokio::test]
    async fn split_off_errors_error_terminates_stream() -> Result<()> {
        use std::io::{Error, ErrorKind};

        // Create a custom reader that errors after some data
        struct ErrorAfterN {
            data: Vec<u8>,
            pos: usize,
            error_at: usize,
        }

        impl AsyncRead for ErrorAfterN {
            fn poll_read(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &mut [u8],
            ) -> Poll<Result<usize>> {
                if self.pos >= self.error_at {
                    return Poll::Ready(Err(Error::new(ErrorKind::Other, "test error")));
                }
                let remaining = self.error_at - self.pos;
                let to_read = buf.len().min(remaining).min(self.data.len() - self.pos);
                if to_read == 0 {
                    return Poll::Ready(Ok(0));
                }
                buf[..to_read].copy_from_slice(&self.data[self.pos..self.pos + to_read]);
                self.pos += to_read;
                Poll::Ready(Ok(to_read))
            }
        }

        impl AsyncWrite for ErrorAfterN {
            fn poll_write(
                self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
                buf: &[u8],
            ) -> Poll<Result<usize>> {
                Poll::Ready(Ok(buf.len()))
            }
            fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<()>> {
                Poll::Ready(Ok(()))
            }
        }

        // Prepare data: one valid message, then error
        let msg1 = wrap_uint24_le(b"hello");
        let mut data = msg1.clone();
        // Add partial data for second message that will error
        data.extend_from_slice(&[0, 0, 5]); // header for 5 byte body

        let error_reader = ErrorAfterN {
            data,
            pos: 0,
            error_at: msg1.len() + 3 + 1, // error after header of second message
        };

        let framed = Uint24LELengthPrefixedFraming::new(error_reader);
        let (mut framed, mut errors) = framed.split_off_errors();

        // First message should succeed
        let first = framed.next().await;
        assert_eq!(first, Some(b"hello".to_vec()));

        // Second read should error and terminate
        let second = framed.next().await;
        assert!(second.is_none());

        // Error should be in the error channel
        let err = errors.next().now_or_never();
        assert!(err.is_some());

        Ok(())
    }
}
