package klattice.wire.msg;

import java.util.Collection;

public record PgsqlPayload(char command, Collection<MessageField<?>> fields) {}
