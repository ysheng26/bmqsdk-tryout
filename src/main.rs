use std::error::Error;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::{io::AsyncReadExt, io::AsyncWriteExt, net::TcpStream};

struct Reader {
    rx: OwnedReadHalf,
}

impl Reader {
    pub async fn run(mut self) -> Result<(), Box<dyn Error>> {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello, world!");

    let mut stream = TcpStream::connect("127.0.0.1:5555").await?;
    stream.write_all(b"hi").await?;

    let (rx, tx) = stream.into_split();
    let reader = tokio::spawn(async move { Reader { rx } }).await?;
    reader.run().await?;

    Ok(())
}
