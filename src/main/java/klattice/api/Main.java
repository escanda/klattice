package klattice.api;

import io.quarkus.grpc.GrpcService;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.smallrye.mutiny.Uni;

@GrpcService
@QuarkusMain
public class Main implements QuarkusApplication, QueryImplBase {

    public static void main(String[] args) {
        Quarkus.run(Main.class, args);
    }

    @Override
    public Uni<Plan> prepareUni(QueryContext request) {
        return Uni.createFrom().item("Hello " + request.getName() + "!")
                .map(msg -> HelloReply.newBuilder().setMessage(msg).build());
    }

    @Override
    public int run(String... args) throws Exception {
        System.out.println("Running Query service...");
    }

}
