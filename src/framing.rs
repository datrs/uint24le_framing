//! Wrap bytes IO in length prefixed framing. Length is little endian 24 bit unsigned integer.
use crate::util::{stat_uint24_le, wrap_uint24_le};
use futures::{Sink, Stream};
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
#[cfg(test)]
pub(crate) mod test {
    use crate::duplex::Duplex;

    use super::*;
    use futures::{SinkExt, StreamExt};
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
}
