// net.rs
#![allow(warnings)]

use mio::event::Event;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::net::{self, SocketAddr};


#[test]
fn test() -> Result<(), Box<dyn std::error::Error>> {
    // Bind a server socket to connect to.
    let addr: SocketAddr = "127.0.0.1:6000".parse()?;
    let server = net::TcpListener::bind(addr)?;

    // Construct a new `Poll` handle as well as the `Events` we'll store into
    let mut poll = Poll::new()?;
    let mut events = Events::with_capacity(1024);

    // Connect the stream
    let mut stream = TcpStream::connect(server.local_addr()?)?;

    // Register the stream with `Poll`
    poll.registry().register(
        &mut stream,
        Token(0),
        Interest::READABLE | Interest::WRITABLE,
    )?;

    // Wait for the socket to become ready. This has to happens in a loop to
    // handle spurious wakeups.
    loop {
        poll.poll(&mut events, None)?;

        for event in &events {
            dbg!(&event);
            if event.token() == Token(0) && event.is_writable() {
                // The socket connected (probably, it could still be a spurious
                // wakeup)
                //return Ok(());
            }
        }
    }
    Ok(())
}
