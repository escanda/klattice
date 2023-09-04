package klattice.wire.msg.client;

import klattice.wire.msg.Message;
import klattice.wire.msg.PgsqlPayloadProvider;

import java.util.Map;

public record Startup(int protocol, Map<String, String> params) implements Message {
    @Override
    public char command() {
        return 'S';
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return emptyProvider();
    }

    @Override
    public Object[] values() {
        return new Object[0];
    }
}
