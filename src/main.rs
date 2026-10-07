use std::error::Error;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, net::TcpStream};

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
        self.tx.writable().await?;
        self.tx.try_write(b"writer run")?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello, world!");

    let mut stream = TcpStream::connect("127.0.0.1:5555").await?;
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

    let reader_res = reader_handle.await?;
    reader_res.unwrap();

    Ok(())
}
