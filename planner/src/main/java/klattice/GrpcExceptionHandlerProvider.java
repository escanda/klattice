package klattice;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.StatusException;
import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.ExceptionHandlerProvider;
import io.quarkus.runtime.Startup;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

@ApplicationScoped
@Startup
public class GrpcExceptionHandlerProvider implements ExceptionHandlerProvider {
    @LoggerName("GrpcExceptionHandlerProvider")
    Logger logger;

    @Override
    public <ReqT, RespT> io.quarkus.grpc.ExceptionHandler<ReqT, RespT> createHandler(ServerCall.Listener<ReqT> listener, ServerCall<ReqT, RespT> serverCall, Metadata metadata) {
        return new KlatticeExceptionHandler<>(logger, listener, serverCall, metadata);
    }

    private static class KlatticeExceptionHandler<ReqT, RespT> extends io.quarkus.grpc.ExceptionHandler<ReqT, RespT> {
        private final Logger logger;

        public KlatticeExceptionHandler(Logger logger, ServerCall.Listener<ReqT> listener, ServerCall<ReqT, RespT> call,
                                        Metadata metadata) {
            super(listener, call, metadata);
            this.logger = logger;
        }

        @Override
        protected void handleException(Throwable exception, ServerCall<ReqT, RespT> call, Metadata metadata) {
            logger.error(exception.getMessage(), exception);
            StatusException se = (StatusException) ExceptionHandlerProvider.toStatusException(exception, false);
            Metadata trailers = se.getTrailers() != null ? se.getTrailers() : metadata;
            call.close(se.getStatus(), trailers);
        }
    }
}
