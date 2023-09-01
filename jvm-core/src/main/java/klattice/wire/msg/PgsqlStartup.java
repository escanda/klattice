package klattice.wire.msg;

import java.util.Map;

public record PgsqlStartup(int length, int protocol, Map<String, String> params) {}
