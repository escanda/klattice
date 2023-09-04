package klattice.wire.msg;

import java.util.Arrays;
import java.util.Optional;

public enum PgsqlClientCommandType {
    Startup('\0'),
    Bind('B'),
    Close('C'),
    CopyData('d'),
    CopyDone('c'),
    CopyFail('f'),
    Describe('D'),
    Execute('E'),
    Flush('H'),
    FunctionCall('F'),
    Parse('P'),
    Password('p'),
    Query('Q'),
    Sync('S'),
    Terminate('X');

    public final char id;

    PgsqlClientCommandType(Character id) {
        this.id = id;
    }

    public static Optional<PgsqlClientCommandType> from(Character id) {
        return Arrays.stream(PgsqlClientCommandType.values()).filter(cmd -> cmd.id == id).findFirst();
    }
}
