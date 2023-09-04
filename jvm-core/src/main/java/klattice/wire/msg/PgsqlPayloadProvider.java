package klattice.wire.msg;

@FunctionalInterface
public interface PgsqlPayloadProvider {
    PgsqlPayload payload();
}
