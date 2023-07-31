use tonic;

pub mod substrait {
    tonic::include_proto!("substrait");

    pub mod extensions {
        tonic::include_proto!("substrait.extensions");
    }
}

pub mod klattice {
    pub mod api {
        tonic::include_proto!("klattice.api");
    }
    pub mod plan {
        tonic::include_proto!("klattice.plan");
    }
}
