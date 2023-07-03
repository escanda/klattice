use tonic;

tonic::include_proto!("substrait");

pub mod klattice {
    pub mod api {
        tonic::include_proto!("klattice.api");
    }
    pub mod plan {
        tonic::include_proto!("klattice.plan");
    }
}
