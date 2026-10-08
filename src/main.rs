use std::error::Error;
use std::io::ErrorKind;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::{io::AsyncReadExt, net::TcpStream};

struct Reader {
    rx: OwnedReadHalf,
}

impl Reader {
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut buffer = [0; 10];

        loop {
            let bytes_read = self.rx.read(&mut buffer).await?;
            if bytes_read == 0 {
                println!("no more");
                return Ok(());
            }
            println!("{:?}", &buffer[..bytes_read]);
        }
    }
}

struct Writer {
    tx: OwnedWriteHalf,
}

impl Writer {
    pub async fn run(self) -> Result<(), Box<dyn Error + Send + Sync>> {
        //
        let payload = b"writer run";
        let mut total_bytes_wrote = 0;
        while total_bytes_wrote < payload.len() {
            self.tx.writable().await?;
            let res = self.tx.try_write(&payload[total_bytes_wrote..]);
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

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Hello, world!");

    let stream = TcpStream::connect("127.0.0.1:5555").await?;
    // stream.write_all(b"hi").await?;

    let (rx, tx) = stream.into_split();
    let reader_handle = tokio::spawn(async move {
        let reader = Reader { rx };
        reader.run().await
    });

    let writer_handle = tokio::spawn(async move {
        let writer = Writer { tx };
        writer.run().await
    });

    let writer_res = writer_handle.await?;
    writer_res?;

    let reader_res = reader_handle.await?;
    reader_res?;

    Ok(())
}
