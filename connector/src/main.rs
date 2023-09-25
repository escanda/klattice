use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use klattice::msg::{Batch, Query};
use pgwire::messages::response;
use substrait::r#type::{Boolean, I8, I16};
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::{ClientInfo, MakeHandler, StatelessMakeHandler, Type};
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

use klattice::oracle::oracle_service_client::OracleServiceClient;
use tonic::transport::Channel;

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
    oracle: Box<OracleServiceClient<Channel>>,
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
        let mut oracle_client = self.oracle.clone();
        let mut request = Query::default();
        request.query = String::from(query_str);

        return match oracle_client.answer(request).await {
            Ok(mut response) => {
                let batch = response.get_mut();
                let pg_field_types: Vec<Option<Type>> = batch.field_types.iter()
                    .map(|field_type| field_type.kind.as_ref().map(|kind| match kind {
                        Boolean => Type::BOOL,
                        I8 => Type::INT8,
                        I16 => Type::INT8,
                    })
                ).collect();
                let field_infos: Vec<FieldInfo> = batch.field_names.iter()
                    .zip(pg_field_types)
                    .map(|(field_name, field_type_opt)|
                        FieldInfo::new(field_name.into(), None, None, field_type_opt.unwrap(), FieldFormat::Text))
                    .collect();
                let schema = Arc::new(field_infos);
                let schema_ref = schema.clone();
                let mut encoder = DataRowEncoder::new(schema_ref.clone());
                encoder.encode_field(&1)?;
                let row = encoder.finish();
                let data_row_stream = stream::iter(vec![row]);

                Ok(vec![Response::Query(QueryResponse::new(
                    schema,
                    data_row_stream,
                ))])
            },
            Err(err) => PgWireResult::Err(pgwire::error::PgWireError::ApiError(Box::new(err)))
        }
    }
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let oracle = Box::new(OracleServiceClient::connect("http://[::1]:9000").await?);
    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(QueryProcessor { oracle })));
    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(StatelessMakeHandler::new(Arc::new(
        PlaceholderExtendedQueryHandler,
    )));
    let authenticator = Arc::new(StatelessMakeHandler::new(Arc::new(NoopStartupHandler)));

    let server_addr = "127.0.0.1:5433";
    let listener = TcpListener::bind(server_addr).await?;
    println!("Listening to {}", server_addr);
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
