use std::error::Error;
use std::io::ErrorKind;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::{io::AsyncReadExt, net::TcpStream};

struct Reader {
    from_tcp_rx: OwnedReadHalf,
    to_coordinator_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl Reader {
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut buffer = [0; 10];

        loop {
            let bytes_read = self.from_tcp_rx.read(&mut buffer).await?;
            if bytes_read == 0 {
                println!("no more");
                return Ok(());
            }
            let payload = buffer[..bytes_read].to_vec();
            self.to_coordinator_tx.send(payload).await?;
            println!("{:?}", &buffer[..bytes_read]);
        }
    }
}

struct Writer {
    tcp_tx: OwnedWriteHalf,
    ch_rx: mpsc::Receiver<Vec<u8>>,
}

impl Writer {
    pub async fn run(mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        while let Some(payload) = self.ch_rx.recv().await {
            let mut total_bytes_wrote = 0;
            while total_bytes_wrote < payload.len() {
                self.tcp_tx.writable().await?;
                let res = self.tcp_tx.try_write(&payload[total_bytes_wrote..]);
                match res {
                    Ok(0) => return Err(std::io::Error::from(ErrorKind::WriteZero).into()),
                    Ok(bytes_wrote) => total_bytes_wrote += bytes_wrote,
                    Err(e) => {
                        if e.kind() == ErrorKind::WouldBlock {
                            continue;
                        } else {
                            return Err(e.into());
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

struct Client {
    to_coordinator_tx: mpsc::Sender<Vec<u8>>,
    // from_coordinator_rx: mpsc::Receiver<Vec<u8>>,
}

impl Client {
    pub async fn send(&self, payload: Vec<u8>) -> Result<(), Box<dyn Error + Sync + Send>> {
        self.to_coordinator_tx.send(payload).await?;
        Ok(())
    }
}

struct Coordinator {
    // client -> coordinator -> writer -> tcp
    from_client_rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    to_writer_tx: tokio::sync::mpsc::Sender<Vec<u8>>,

    // tcp -> reader -> coordinator -> client
    from_reader_rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    to_client_tx: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl Coordinator {
    pub async fn run(mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        loop {
            tokio::select! {
                maybe_payload = self.from_client_rx.recv() => {
                    if let Some(payload) = maybe_payload {
                        self.to_writer_tx.send(payload).await?;
                    } else {
                        return Ok(());
                    }
                }
                maybe_payload = self.from_reader_rx.recv() => {
                    if let Some(payload) = maybe_payload {
                        println!("coordinator: from_raeder_rx payload {:?}", payload);
                        self.to_client_tx.send(payload).await?;
                    } else {
                        return Ok(());
                    }
                }
            }
        }
    }
}

// send flow
// client -> coordinator -> writer -> tcp
//
// read flow
// tcp -> reader -> coordinator -> client
//
// state machine in coordinator
// the client is the remote control

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Hello, world!");

    let stream = TcpStream::connect("127.0.0.1:5555").await?;
    // stream.write_all(b"hi").await?;

    let (client_to_coordinator_tx, client_to_coordinator_rx) = tokio::sync::mpsc::channel(32);
    let (coordinator_to_writer_tx, coordinator_to_writer_rx) = tokio::sync::mpsc::channel(32);
    let (reader_to_coordinator_tx, reader_to_coordinator_rx) = tokio::sync::mpsc::channel(32);
    let (coordinator_to_client_tx, mut coordinator_to_client_rx) = tokio::sync::mpsc::channel(32);

    let (tcp_rx, writer_to_tcp_tx) = stream.into_split();
    let reader_handle = tokio::spawn(async move {
        let reader = Reader {
            from_tcp_rx: tcp_rx,
            to_coordinator_tx: reader_to_coordinator_tx,
        };
        reader.run().await
    });

    // client to coordinator
    let client = Client {
        to_coordinator_tx: client_to_coordinator_tx,
        // from_coordinator_rx: coordinator_to_client_rx,
    };

    let coordinator_handle = tokio::spawn(async move {
        let coordinator = Coordinator {
            from_client_rx: client_to_coordinator_rx,
            to_writer_tx: coordinator_to_writer_tx,

            from_reader_rx: reader_to_coordinator_rx,
            to_client_tx: coordinator_to_client_tx,
        };
        coordinator.run().await
    });

    // coordinator to writer
    let writer_handle = tokio::spawn(async move {
        let writer = Writer {
            tcp_tx: writer_to_tcp_tx,
            ch_rx: coordinator_to_writer_rx,
        };
        writer.run().await
    });

    client.send(b"handshake from client".to_vec()).await?;

    loop {
        let res = coordinator_to_client_rx.recv().await;
        if let Some(payload) = res {
            println!("got payload from client response {:?}", payload);
        } else {
            break;
        }
    }

    let reader_res = reader_handle.await?;
    reader_res?;

    drop(client);

    let coordinator_res = coordinator_handle.await?;
    coordinator_res?;

    let writer_res = writer_handle.await?;
    writer_res?;

    Ok(())
}
