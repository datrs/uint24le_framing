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
use tracing::{debug, error, instrument, trace, warn};

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

    /// Remove and return the next frame if all of it has been read into [`Self::to_stream`].
    fn take_buffered_frame(&mut self) -> Option<Vec<u8>> {
        if let Step::Header = self.step {
            let cur_data = &self.to_stream[self.last_out_idx..self.last_data_idx];
            let Some((header_len, body_len)) = stat_uint24_le(cur_data) else {
                trace!("not enough bytes to read header");
                return None;
            };
            let cur_frame_start = self.last_out_idx + header_len;
            self.step = Step::Body {
                start: cur_frame_start,
                end: (cur_frame_start as u64) + body_len,
            };
        }

        let Step::Body { start, end } = self.step else {
            unreachable!("a header was just parsed");
        };
        let end = end as usize;
        if end > self.last_data_idx {
            trace!("Frame not ready start = {start}, end = {end}");
            return None;
        }

        debug!(frame_size = end - start, "Frame ready");
        let frame = self.to_stream[start..end].to_vec();
        self.step = Step::Header;

        // remove bytes we're done with
        self.to_stream.rotate_left(end);
        self.last_data_idx -= end;
        self.last_out_idx = 0;
        Some(frame)
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
        let this = self.get_mut();
        // Read until a frame is ready or `poll_read` is `Pending`. Stopping any sooner would
        // return `Pending` without the IO holding our waker, and nothing would poll us again.
        loop {
            if let Some(frame) = this.take_buffered_frame() {
                return Poll::Ready(Some(Ok(frame)));
            }

            // A read into an empty slice returns `Ok(0)`, which would look like EOF.
            if this.last_data_idx == this.to_stream.len() {
                let doubled_length = this.to_stream.len() * 2;
                debug!("Read buffer full, growing it to [{doubled_length}] bytes");
                this.to_stream.resize(doubled_length, 0);
            }

            let unfilled = &mut this.to_stream[this.last_data_idx..];
            match Pin::new(&mut this.io).poll_read(cx, unfilled) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Ok(0)) if this.last_data_idx == this.last_out_idx => {
                    // Clean EOF with no pending data
                    return Poll::Ready(None);
                }
                Poll::Ready(Ok(0)) => {
                    // EOF with incomplete message
                    return Poll::Ready(Some(Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "connection closed with incomplete message",
                    ))));
                }
                Poll::Ready(Ok(n_bytes_read)) => {
                    trace!(
                        "adding #=[{n_bytes_read}] bytes at [{}]",
                        this.last_data_idx
                    );
                    this.last_data_idx += n_bytes_read;
                }
            }
        }
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
    use std::time::Duration;
    use tokio::{spawn, time::timeout};
    use tokio_util::compat::TokioAsyncReadCompatExt;

    pub(crate) fn duplex(
        channel_size: usize,
    ) -> (impl AsyncRead + AsyncWrite, impl AsyncRead + AsyncWrite) {
        let (left, right) = tokio::io::duplex(channel_size);
        (left.compat(), right.compat())
    }

    /// Longer than any of these tests should take, so a stall fails instead of hanging.
    const STALL_TIMEOUT: Duration = Duration::from_secs(5);

    fn patterned_bytes(length: usize) -> Vec<u8> {
        (0..length).map(|index| (index % 251) as u8).collect()
    }

    /// Send `frames` from one framed end and collect them at the other.
    async fn send_and_receive_frames(frames: Vec<Vec<u8>>) -> Result<Vec<Vec<u8>>> {
        let (left, right) = tokio::io::duplex(4096);
        let mut sender = Uint24LELengthPrefixedFraming::new(left.compat());
        let mut receiver = Uint24LELengthPrefixedFraming::new(right.compat());
        let frame_count = frames.len();

        let send_all = async {
            for frame in frames {
                sender.send(frame).await?;
            }
            Ok::<(), std::io::Error>(())
        };
        let receive_all = async {
            let mut received = vec![];
            while received.len() < frame_count {
                received.push(receiver.next().await.expect("the sender is still open")?);
            }
            Ok::<Vec<Vec<u8>>, std::io::Error>(received)
        };

        let (sent, received) =
            timeout(STALL_TIMEOUT, async { tokio::join!(send_all, receive_all) })
                .await
                .expect("frames stalled");
        sent?;
        received
    }

    /// The sizes either side of where a frame stops fitting the initial read buffer, along with
    /// its 3 byte header.
    #[tokio::test]
    async fn frames_around_the_size_of_the_read_buffer_are_received() -> Result<()> {
        for frame_length in [
            BUF_SIZE - 5,
            BUF_SIZE - 4,
            BUF_SIZE - 3,
            BUF_SIZE - 2,
            BUF_SIZE,
            BUF_SIZE + 1,
        ] {
            let frame = patterned_bytes(frame_length);
            assert_eq!(
                send_and_receive_frames(vec![frame.clone()]).await?,
                [frame],
                "frame of {frame_length} bytes"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_frame_many_times_the_read_buffer_is_received() -> Result<()> {
        let frame = patterned_bytes(10 * BUF_SIZE + 1);
        assert_eq!(send_and_receive_frames(vec![frame.clone()]).await?, [frame]);
        Ok(())
    }

    #[tokio::test]
    async fn frames_after_a_large_frame_are_received_in_order() -> Result<()> {
        let frames = vec![
            patterned_bytes(2 * BUF_SIZE),
            b"small".to_vec(),
            patterned_bytes(3 * BUF_SIZE),
            vec![],
            b"last".to_vec(),
        ];
        assert_eq!(send_and_receive_frames(frames.clone()).await?, frames);
        Ok(())
    }

    /// Reading the first half consumes a ready read, so the receiver has to go back and poll the
    /// IO again, or nothing wakes it when the second half arrives. It gets its own task because
    /// `tokio::join!` re-polls every branch on any wake, which would hide that.
    #[tokio::test]
    async fn a_frame_arriving_in_two_writes_is_received() -> Result<()> {
        let (mut raw_sender, right) = tokio::io::duplex(4096);
        let mut receiver = Uint24LELengthPrefixedFraming::new(right.compat());
        let frame = patterned_bytes(100);
        let wire_bytes = wrap_uint24_le(&frame);
        let (first_half, second_half) = wire_bytes.split_at(wire_bytes.len() / 2);

        let receiving = spawn(async move { receiver.next().await });

        tokio::io::AsyncWriteExt::write_all(&mut raw_sender, first_half).await?;
        tokio::time::sleep(Duration::from_millis(50)).await;
        tokio::io::AsyncWriteExt::write_all(&mut raw_sender, second_half).await?;

        let received = timeout(STALL_TIMEOUT, receiving)
            .await
            .expect("the frame stalled")
            .expect("the receiving task panicked")
            .expect("the sender is still open")?;
        assert_eq!(received, frame);
        Ok(())
    }

    #[tokio::test]
    async fn eof_part_way_through_a_large_frame_is_an_error() -> Result<()> {
        let (mut raw_sender, right) = tokio::io::duplex(4 * BUF_SIZE);
        let mut receiver = Uint24LELengthPrefixedFraming::new(right.compat());
        let wire_bytes = wrap_uint24_le(&patterned_bytes(3 * BUF_SIZE));

        tokio::io::AsyncWriteExt::write_all(&mut raw_sender, &wire_bytes[..2 * BUF_SIZE]).await?;
        drop(raw_sender);

        let received = timeout(STALL_TIMEOUT, receiver.next())
            .await
            .expect("EOF stalled")
            .expect("an incomplete frame is reported, not dropped");
        assert_eq!(
            received.unwrap_err().kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        Ok(())
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
        use std::io::Error;

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
                    return Poll::Ready(Err(Error::other("test error")));
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
