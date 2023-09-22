package klattice.wire.msg;

import java.util.Arrays;
import java.util.Optional;

public enum PgsqlServerCommandType {
    AuthenticationOk('R'),
    AuthenticationKerberosV5('R'),
    AuthenticationCleartextPassword('R'),
    AuthenticationMD5Password('R'),
    AuthenticationSCMCredential('R'),
    AuthenticationGSS('R'),
    AuthenticationSSPI('R'),
    AuthenticationGSSContinue('R'),
    AuthenticationSASL('R'),
    AuthenticationSASLContinue('R'),
    AuthenticationSASLFinal('R'),
    BackendKeyData('K'),
    BindComplete('2'),
    CloseComplete('3'),
    CommandComplete('C'),
    CopyData('d'),
    CopyDone('c'),
    CopyInResponse('G'),
    CopyOutResponse('H'),
    CopyBothResponse('W'),
    DataRow('D'),
    EmptyQueryResponse('I'),
    ErrorResponse('E'),
    FunctionCallResponse('V'),
    NegotiateProtocolVersion('v'),
    NoData('n'),
    NoticeResponse('N'),
    NotificationResponse('A'),
    ParameterDescription('t'),
    ParameterStatus('S'),
    ParseComplete('1'),
    PortalSuspended('s'),
    ReadyForQuery('Z'),
    RowDescription('T');

    public final char id;

    PgsqlServerCommandType(char id) {
        this.id = id;
    }

    public static Optional<PgsqlServerCommandType> from(char id) {
        return Arrays.stream(PgsqlServerCommandType.values()).filter(cmd -> cmd.id == id).findFirst();
    }
}
