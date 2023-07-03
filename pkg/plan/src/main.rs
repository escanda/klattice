use std::error::Error;
use std::io::Write;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};
use tonic::{transport::Server, Request, Response, Status};
use tonic_health::server::HealthReporter;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncRead};
use klattice_api::substrait::*;
use klattice_api::klattice::api::*;
use klattice_api::klattice::api::query_server::{Query, QueryServer};

#[derive(Default)]
pub struct QueryPlanner {}

#[allow(implied_bounds_entailment)]
#[tonic::async_trait]
impl Query for QueryPlanner {
    async fn prepare(&self, request: Request<QueryContext>) ->  Result<Response<Plan>, Status> {
        todo!()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut stdout = StandardStream::stdout(ColorChoice::Always);
    stdout.set_color(ColorSpec::new().set_fg(Some(Color::Green)))?;
    writeln!(&mut stdout, "Starting planner...")?;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    
    let addr = "[::1]:50051".parse().unwrap();

    let qserv = QueryServer::new(QueryPlanner::default());

    Server::builder()
        .add_service(health_service)
        .add_service(qserv)
        .serve(addr)
        .await?;
    Ok(())
}