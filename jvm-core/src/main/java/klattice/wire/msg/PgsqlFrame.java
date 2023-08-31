package klattice.wire.msg;

import io.netty.buffer.ByteBuf;

public record PgsqlFrame(PgsqlServerCommandType command,
                         int length,
                         ByteBuf contents) {}
