use std::error::Error;
use std::io::ErrorKind;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::{io::AsyncReadExt, net::TcpStream};

struct Reader {
    tcp_rx: OwnedReadHalf,
}

impl Reader {
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut buffer = [0; 10];

        loop {
            let bytes_read = self.tcp_rx.read(&mut buffer).await?;
            if bytes_read == 0 {
                println!("no more");
                return Ok(());
            }
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
    ch_tx: mpsc::Sender<Vec<u8>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Hello, world!");

    let stream = TcpStream::connect("127.0.0.1:5555").await?;
    // stream.write_all(b"hi").await?;

    let (tcp_rx, tcp_tx) = stream.into_split();
    let reader_handle = tokio::spawn(async move {
        let reader = Reader { tcp_rx };
        reader.run().await
    });

    let (ch_tx, ch_rx) = tokio::sync::mpsc::channel(32);

    let writer_handle = tokio::spawn(async move {
        let writer = Writer { tcp_tx, ch_rx };
        writer.run().await
    });

    // let client = ;

    let writer_res = writer_handle.await?;
    writer_res?;

    let reader_res = reader_handle.await?;
    reader_res?;

    Ok(())
}
