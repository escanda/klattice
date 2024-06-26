use std::env;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use klattice::msg::Query;
use pgwire::messages::data::DataRow;
use substrait::r#type::Kind;
use tokio::net::TcpListener;

use futures::stream;
use futures::StreamExt;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

use klattice::oracle::oracle_service_client::OracleServiceClient;

pub mod substrait {
    tonic::include_proto!("substrait");

    pub mod extensions {
        tonic::include_proto!("substrait.extensions");
    }
}

pub mod klattice {
    pub mod msg {
        tonic::include_proto!("klattice.msg");
    }
    pub mod exec {
        tonic::include_proto!("klattice.exec");
    }
    pub mod plan {
        tonic::include_proto!("klattice.plan");
    }
    pub mod query {
        tonic::include_proto!("klattice.query");
    }
    pub mod oracle {
        tonic::include_proto!("klattice.facade");
    }
}

pub struct QueryProcessor {
    oracle_addr: String,
}

#[async_trait]
impl SimpleQueryHandler for QueryProcessor {
    async fn do_query<'a, 'b, C>(
        &'b self,
        _client: &C,
        query_str: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        println!(
            "Querying '{}' to server: '{}'",
            query_str,
            self.oracle_addr.clone()
        );

        let mut oracle_client = match OracleServiceClient::connect(self.oracle_addr.clone()).await {
            Ok(client) => client,
            Err(err) => panic!(
                "Cannot connect to query oracle because '{}'. Exiting thread",
                err
            ),
        };
        let mut request = Query::default();
        request.query = String::from(query_str);

        return match oracle_client.answer(request).await {
            Ok(response) => {
                let batch = response.into_inner();
                let field_infos: Vec<FieldInfo> = batch
                    .columns
                    .iter()
                    .map(|col| {
                        let col_type = col
                            .r#type
                            .clone()
                            .map(|_type| match _type.kind.unwrap() {
                                Kind::Bool(_) => Type::BOOL,
                                Kind::I8(_) => Type::INT2,
                                Kind::I16(_) => Type::INT2,
                                Kind::I32(_) => Type::INT4,
                                Kind::Fp32(_) => Type::FLOAT4,
                                Kind::Fp64(_) => Type::FLOAT8,
                                Kind::I64(_) => Type::INT4,
                                Kind::String(_) => Type::VARCHAR,
                                Kind::Binary(_) => Type::BIT_ARRAY,
                                Kind::Timestamp(_) => Type::TIMESTAMP,
                                Kind::Date(_) => Type::DATE,
                                Kind::Time(_) => Type::TIME,
                                Kind::IntervalYear(_) => Type::INTERVAL,
                                Kind::IntervalDay(_) => Type::INTERVAL,
                                Kind::TimestampTz(_) => Type::TIMESTAMPTZ,
                                Kind::Uuid(_) => Type::UUID,
                                Kind::FixedChar(_) => Type::CHAR,
                                Kind::Varchar(_) => Type::VARCHAR,
                                Kind::FixedBinary(_) => Type::BIT,
                                Kind::Decimal(_) => Type::NUMERIC,
                                Kind::Struct(_) => Type::UNKNOWN,
                                Kind::List(_) => Type::ANYARRAY,
                                Kind::Map(_) => Type::UNKNOWN,
                                Kind::UserDefined(_) => Type::UNKNOWN,
                                Kind::UserDefinedTypeReference(_) => Type::UNKNOWN,
                            })
                            .unwrap();
                        FieldInfo::new(
                            col.column_name.clone(),
                            None,
                            None,
                            col_type,
                            FieldFormat::Text,
                        )
                    })
                    .collect();
                let schema = Arc::new(field_infos);
                let data_row_stream = stream::iter(batch.rows).map(|row| {
                    Result::Ok(DataRow::new(
                        row.fields
                            .iter()
                            .map(|field_value| Some(bytes::Bytes::from_iter(field_value.clone())))
                            .collect::<Vec<Option<Bytes>>>(),
                    ))
                });
                Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    data_row_stream,
                ))])
            }
            Err(err) => {
                println!(
                    "Cannot execute succesfully query to gRPC endpoint because {}",
                    err
                );
                return PgWireResult::Err(pgwire::error::PgWireError::ApiError(Box::new(err)));
            }
        };
    }
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    const ENDPOINT_ENV_VAR: &str = "KLATTICE_ENDPOINT";
    const LISTENING_ENV_VAR: &str = "KLATTICE_PORT";

    let client_addr = match env::var(ENDPOINT_ENV_VAR) {
        Ok(client_addr) => client_addr,
        Err(_) => "127.0.0.1:8080".to_string(),
    };
    let listen_port_str = match env::var(LISTENING_ENV_VAR) {
        Ok(port_str) => port_str,
        Err(_) => "5433".to_string(),
    };
    let server_addr = format!("{}:{}", "[::]", listen_port_str);
    let listener = TcpListener::bind(server_addr).await?;
    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(QueryProcessor {
        oracle_addr: client_addr,
    })));
    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));
    println!("Listening to {}:{}", "[[::]]", listen_port_str);
    loop {
        let (tcp_stream, _) = listener.accept().await?;
        let authenticator_ref = authenticator.make();
        let processor_ref = processor.make();
        let placeholder_ref = placeholder.make();
        tokio::spawn(async move {
            process_socket(
                tcp_stream,
                None,
                authenticator_ref,
                processor_ref,
                placeholder_ref,
            )
            .await
        });
    }
}
