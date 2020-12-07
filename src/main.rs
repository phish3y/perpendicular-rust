use std::env;
use std::fs::File;
use std::io::{self, BufRead, Read, Write};
use std::path::Path;
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() -> std::io::Result<()> {
    let args: Vec<String> = env::args().collect();

    if args[1].eq_ignore_ascii_case("leader") {
        println!("I am the leader");
        if let Ok(lines) = read_lines("/home/evan/dev/perpendicular-rust/resources/test.csv") {
            let lines_per_node = 5;
            let mut vec_lines = vec![];
            for line in lines {
                if let Ok(ip) = line {
                    vec_lines.push(ip);
                }
            }

            match TcpStream::connect("54.187.129.214:4999") {
                Err(e) => println!("Error: {}", e),
                Ok(mut stream) => {
                    println!("Connected!");
                    for line in vec_lines {
                        stream.write(line.as_bytes()).unwrap();
                    }
                }
            }
        } else {
            println!("Failed to find file!");
        }
    } else {
        println!("I am a worker");
        let listener = TcpListener::bind("0.0.0.0:4999")?;

        for streams in listener.incoming() {
            thread::spawn(move || {
                match streams {
                    Err(e) => println!("Error: {}", e),
                    Ok(stream) => handle_stream(stream).unwrap()
                }
            });
        }
    }

    Ok(())
}

fn handle_stream(mut stream: TcpStream) -> Result<(), std::io::Error> {
    println!("Connection from: {}", stream.peer_addr()?);

    let mut buffer = [0; 8192];
    //loop {
        let nbytes = stream.read(&mut buffer)?;
        if nbytes == 0 {
            return Ok(());
        }

        for i in 0..nbytes {
            match std::str::from_utf8(&buffer) {
                Err(e) => println!("Error: {}", e),
                Ok(result) => {
                    print!("{}", result);
                }
            }
        }
        println!("");

        stream.flush()?;
    //}

    Ok(())
}


fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>> where P: AsRef<Path>, {
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}