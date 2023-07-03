use std::io::Write;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use tonic::{transport::Server, Request, Response, Status};
use tonic_health::server::HealthReporter;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use klattice_api::api::{Query, QueryServer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    stdout.set_color(ColorSpec::new().set_fg(Some(Color::Green)))?;
    writeln!(&mut stdout, "Starting planner...")?;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    
    let addr = "[::1]:50051".parse().unwrap();

    Server::builder()
        .add_service(health_service)
        .serve(addr)
        .await?;
    Ok(())
}