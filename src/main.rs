// Check when to flush the sended data if no one awaits the future

use std::{pin::Pin, task::{Context, Poll}, time::Duration, time::Instant};
use anyhow::Result;
use tokio::{task, net::{TcpListener, TcpStream, tcp}, io::{BufReader, AsyncBufReadExt, BufWriter}, time::{sleep}};
use tokio_stream::wrappers::LinesStream;
use futures::{ready, Stream, Sink, StreamExt, SinkExt, AsyncWriteExt, io::IntoSink};
use async_compat::Compat;
use serde_json::{Value, json};
use serde::{Serialize, Deserialize};

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let server_task =task::spawn(json_protocol_server());
    let client_task = task::spawn(json_protocol_client());
    let (server_result, client_result) = tokio::join!(server_task, client_task);
    server_result??;
    client_result??;
    Ok(())
}

async fn json_protocol_server() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let (stream, _) = listener.accept().await?;
    let mut protocol = HeartbeatProtocol::new(stream, "Server");
    while let Some(value) = protocol.next().await {
        let value = value?;
        log::info!("Server received: {:?}", value);
    }
    Ok(())
}

async fn json_protocol_client() -> Result<()> {
    sleep(Duration::from_secs(2)).await;
    let stream = TcpStream::connect("127.0.0.1:8080").await?;
    let mut protocol = HeartbeatProtocol::new(stream, "Client");
    // next を必須にしないためには poll_ready, poll_flush でもプロトコルの下位スタックに poll_next を呼ぶ必要がある
    // packet の queue を用意して
    loop {
        tokio::select! {
            value = protocol.next() => {
                unreachable!("Client only needs to be receiving for heartbeat mechanism, but received: {:?}", value);
            },
            _ = sleep(Duration::from_secs(1)) => {
                protocol.send(json!({"message": "Hello"})).await?;
            },
        }
    };
}

#[derive(Debug, Serialize, Deserialize)]
struct HeartbeatPacket {
    kind: HeartbeatPacketKind,
    value: Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
enum HeartbeatPacketKind {
    Ping,
    Pong,
    Message,
}

struct HeartbeatProtocol {
    json_protocol: JsonProtocol,
    ping_stage: HeartbeatPingStage,
    pong_stage: HeartbeatPongStage,
    role: String,
}

#[derive(Debug, Clone, Copy)]
enum HeartbeatPingStage {
    WaitingUntil(Instant),
    WaitingReady,
    SendingStarted,
    WaitingFlushed,
}

#[derive(Debug, Clone, Copy)]
enum HeartbeatPongStage {
    NoNeeded,
    WaitingReady,
    SendingStarted,
    WaitingFlushed,
}

impl HeartbeatProtocol {
    fn new(stream: TcpStream, role: impl Into<String>) -> Self {
        let json_protocol = JsonProtocol::new(stream);
        Self {
            json_protocol,
            ping_stage: HeartbeatPingStage::WaitingUntil(Instant::now() + Duration::from_secs(5)),
            pong_stage: HeartbeatPongStage::NoNeeded,
            role: role.into(),
        }
    }
}

impl HeartbeatProtocol {
    fn poll_ping(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = &mut *self;
        loop {
            let json_protocol = Pin::new(&mut this.json_protocol);
            match this.ping_stage {
                HeartbeatPingStage::WaitingUntil(when) => {
                    log::trace!("[{}] HeartbeatProtocol::poll_ping: WaitingUntil({:?})", this.role, when);
                    if Instant::now() >= when {
                        this.ping_stage = HeartbeatPingStage::WaitingReady;
                    } else {
                        return Poll::Ready(Ok(()));
                    }
                },
                HeartbeatPingStage::WaitingReady => {
                    log::trace!("[{}] HeartbeatProtocol::poll_ping: WaitingReady", this.role);
                    match json_protocol.poll_ready(cx) {
                        Poll::Ready(Ok(())) => {
                            this.ping_stage = HeartbeatPingStage::SendingStarted;
                        },
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(Err(err.into()));
                        },
                        Poll::Pending => {
                            return Poll::Pending;
                        },
                    }
                },
                HeartbeatPingStage::SendingStarted => {
                    log::trace!("[{}] HeartbeatProtocol::poll_ping: SendingStarted", this.role);
                    match serde_json::to_value(HeartbeatPacket { kind: HeartbeatPacketKind::Ping, value: json!({}) }) {
                        Ok(value) => {
                            match json_protocol.start_send(value) {
                                Ok(()) => {
                                    this.ping_stage = HeartbeatPingStage::WaitingFlushed;
                                },
                                Err(err) => {
                                    return Poll::Ready(Err(err.into()));
                                },
                            }
                        },
                        Err(err) => {
                            return Poll::Ready(Err(err.into()));
                        },
                    }
                },
                HeartbeatPingStage::WaitingFlushed => {
                    log::trace!("[{}] HeartbeatProtocol::poll_ping: WaitingFlushed", this.role);
                    match json_protocol.poll_flush(cx) {
                        Poll::Ready(Ok(())) => {
                            this.ping_stage = HeartbeatPingStage::WaitingUntil(Instant::now() + Duration::from_secs(5));
                            break;
                        },
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(Err(err.into()));
                        },
                        Poll::Pending => {
                            return Poll::Pending;
                        },
                    }
                },
            }
        };
        return Poll::Ready(Ok(()));
    }

    fn poll_pong(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = &mut *self;
        loop {
            let json_protocol = Pin::new(&mut this.json_protocol);
            match this.pong_stage {
                HeartbeatPongStage::NoNeeded => {
                    log::trace!("[{}] HeartbeatProtocol::poll_pong: NoNeeded", this.role);
                    return Poll::Ready(Ok(()));
                },
                HeartbeatPongStage::WaitingReady => {
                    log::trace!("[{}] HeartbeatProtocol::poll_pong: WaitingReady", this.role);
                    match json_protocol.poll_ready(cx) {
                        Poll::Ready(Ok(())) => {
                            this.pong_stage = HeartbeatPongStage::SendingStarted;
                        },
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(Err(err.into()));
                        },
                        Poll::Pending => {
                            return Poll::Pending;
                        },
                    }
                },
                HeartbeatPongStage::SendingStarted => {
                    log::trace!("[{}] HeartbeatProtocol::poll_pong: SendingStarted", this.role);
                    match serde_json::to_value(HeartbeatPacket { kind: HeartbeatPacketKind::Pong, value: json!({}) }) {
                        Ok(value) => {
                            match json_protocol.start_send(value) {
                                Ok(()) => {
                                    this.pong_stage = HeartbeatPongStage::WaitingFlushed;
                                },
                                Err(err) => {
                                    return Poll::Ready(Err(err.into()));
                                },
                            }
                        },
                        Err(err) => {
                            return Poll::Ready(Err(err.into()));
                        },
                    }
                },
                HeartbeatPongStage::WaitingFlushed => {
                    log::trace!("[{}] HeartbeatProtocol::poll_pong: WaitingFlushed", this.role);
                    match json_protocol.poll_flush(cx) {
                        Poll::Ready(Ok(())) => {
                            this.pong_stage = HeartbeatPongStage::NoNeeded;
                            break;
                        },
                        Poll::Ready(Err(err)) => {
                            return Poll::Ready(Err(err.into()));
                        },
                        Poll::Pending => {
                            return Poll::Pending;
                        },
                    }
                },
            }
        };
        return Poll::Ready(Ok(()));
    }

    fn poll_protocol_backend(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Result<()> {
        let _ = Pin::new(&mut *self).poll_ping(cx)?;
        let _ = Pin::new(&mut *self).poll_pong(cx)?;
        Ok(())
    }
}

impl Stream for HeartbeatProtocol {
    type Item = Result<Value>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        log::trace!("[{}] HeartbeatProtocol::poll_next", self.role);

        Pin::new(&mut *self).poll_protocol_backend(cx)?;

        let this: &mut Self = &mut *self;
        let json_protocol: &mut _ = &mut this.json_protocol;
        let json_protocol: Pin<&mut _> = Pin::new(json_protocol);
        let value = ready!(json_protocol.poll_next(cx)?);
        let Some(value) = value else {
            return Poll::Ready(None);
        };
        let packet = serde_json::from_value::<HeartbeatPacket>(value)?;
        match packet.kind {
            HeartbeatPacketKind::Ping => {
                log::debug!("[{}] HeartbeatProtocol Received Ping", self.role);
                self.pong_stage = HeartbeatPongStage::WaitingReady;
                // 下位プロトコルの poll_next の ready を pending にするときは wake が必要
                cx.waker().wake_by_ref();
                Poll::Pending
            },
            HeartbeatPacketKind::Pong => {
                log::debug!("[{}] HeartbeatProtocol Received Pong", self.role);
                // 下位プロトコルの poll_next の ready を pending にするときは wake が必要
                cx.waker().wake_by_ref();
                Poll::Pending
            },
            HeartbeatPacketKind::Message => {
                Poll::Ready(Some(Ok(packet.value)))
            },
        }
    }
}

impl Sink<Value> for HeartbeatProtocol {
    type Error = anyhow::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        log::trace!("[{}] HeartbeatProtocol::poll_ready", self.role);
        let this: &mut Self = &mut *self;
        let json_protocol: &mut _ = &mut this.json_protocol;
        let json_protocol: Pin<&mut _> = Pin::new(json_protocol);
        json_protocol.poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Value) -> Result<(), Self::Error> {
        log::trace!("[{}] HeartbeatProtocol::start_send with {:?}", self.role, item);
        let this: &mut Self = &mut *self;
        let json_protocol: &mut _ = &mut this.json_protocol;
        let json_protocol: Pin<&mut _> = Pin::new(json_protocol);
        let packet = serde_json::to_value(HeartbeatPacket { kind: HeartbeatPacketKind::Message, value: item })?;
        json_protocol.start_send(packet)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        log::trace!("[{}] HeartbeatProtocol::poll_flush", self.role);
        let this: &mut Self = &mut *self;
        let json_protocol: &mut _ = &mut this.json_protocol;
        let json_protocol: Pin<&mut _> = Pin::new(json_protocol);
        json_protocol.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        log::trace!("[{}] HeartbeatProtocol::poll_close", self.role);
        let this: &mut Self = &mut *self;
        let json_protocol: &mut _ = &mut this.json_protocol;
        let json_protocol: Pin<&mut _> = Pin::new(json_protocol);
        json_protocol.poll_close(cx)
    }
}

struct JsonProtocol {
    stream: LinesStream<BufReader<tcp::OwnedReadHalf>>,
    sink: IntoSink<Compat<BufWriter<tcp::OwnedWriteHalf>>, String>,
}

impl JsonProtocol {
    fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();
        let stream = LinesStream::new(BufReader::new(reader).lines());
        let sink = Compat::new(BufWriter::new(writer)).into_sink();
        JsonProtocol { stream, sink }
    }
}

impl Stream for JsonProtocol {
    type Item = Result<Value>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // write down all types to be clear
        let this: &mut Self = &mut *self;
        let stream: &mut _ = &mut this.stream;
        let stream: Pin<&mut _> = Pin::new(stream);
        match stream.poll_next(cx) {
            Poll::Ready(Some(Ok(line))) => {
                match serde_json::from_str(&line) {
                    Ok(value) => Poll::Ready(Some(Ok(value))),
                    Err(err) => Poll::Ready(Some(Err(err.into()))),
                }
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Sink<Value> for JsonProtocol {
    type Error = anyhow::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // write down all types to be clear
        let this: &mut Self = &mut *self;
        let sink: &mut _ = &mut this.sink;
        let sink: Pin<&mut _> = Pin::new(sink);
        sink.poll_ready(cx).map_err(|err| err.into())
    }

    fn start_send(mut self: Pin<&mut Self>, item: Value) -> Result<(), Self::Error> {
        // write down all types to be clear
        let this: &mut Self = &mut *self;
        let sink: &mut _ = &mut this.sink;
        let sink: Pin<&mut _> = Pin::new(sink);
        sink.start_send(serde_json::to_string(&item)? + "\n").map_err(|err| err.into())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // write down all types to be clear
        let this: &mut Self = &mut *self;
        let sink: &mut _ = &mut this.sink;
        let sink: Pin<&mut _> = Pin::new(sink);
        sink.poll_flush(cx).map_err(|err| err.into())
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // write down all types to be clear
        let this: &mut Self = &mut *self;
        let sink: &mut _ = &mut this.sink;
        let sink: Pin<&mut _> = Pin::new(sink);
        sink.poll_close(cx).map_err(|err| err.into())
    }
}








#[derive(Debug)]
struct SinkWrapper<S> {
    upstream: S,
}

impl<Item, S> Sink<Item> for SinkWrapper<S>
where
    S: Sink<Item> + Unpin,
    Item: std::fmt::Debug,
{
    type Error = S::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // write down all types to be clear
        let this: &mut Self = &mut *self;
        let upstream: &mut S = &mut this.upstream;
        let upstream: Pin<&mut S> = Pin::new(upstream);
        upstream.poll_ready(cx)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        // write down all types to be clear
        let this: &mut Self = &mut *self;
        let upstream: &mut S = &mut this.upstream;
        let upstream: Pin<&mut S> = Pin::new(upstream);
        upstream.start_send(item)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // write down all types to be clear
        let this: &mut Self = &mut *self;
        let upstream: &mut S = &mut this.upstream;
        let upstream: Pin<&mut S> = Pin::new(upstream);
        upstream.poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // write down all types to be clear
        let this: &mut Self = &mut *self;
        let upstream: &mut S = &mut this.upstream;
        let upstream: Pin<&mut S> = Pin::new(upstream);
        upstream.poll_close(cx)
    }
}

#[allow(dead_code)]
async fn check_no_flushed_sink() -> Result<()> {
    let server_task = task::spawn(server_main());
    let client_task = task::spawn(client_main());
    let (server_result, client_result) = tokio::join!(server_task, client_task);
    server_result??;
    client_result??;
    Ok(())
}

async fn server_main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let (stream, _) = listener.accept().await?;
    let stream = BufReader::new(stream);
    let mut stream = LinesStream::new(stream.lines());

    while let Some(line) = stream.next().await {
        let line = line?;
        println!("Server received: {}", line);
    }

    Ok(())
}

async fn client_main() -> Result<()> {
    let sink = TcpStream::connect("127.0.0.1:8080").await?;
    let sink = Compat::new(sink);
    let sink = sink.into_sink();
    let mut sink = SinkWrapper { upstream: sink };

    sink.send("First message\n").await?; // server received this message

    let _ = sink.send("Second message\n"); // server did not receive this message

    let _f = sink.send("Third message\n"); // serer did not receive this message

    sink.feed("Fourth message\n").await?; // server received this message
    sink.flush().await?;

    sink.feed("Sixth message\n").await?; // server did not receive this message

    sleep(Duration::from_secs(5)).await; // even after 5 seconds

    Ok(())
}

