package klattice.wire.msg.server;

import klattice.wire.hnd.RowFieldType;
import klattice.wire.msg.*;
import klattice.wire.msg.type.Int16V;
import klattice.wire.msg.type.Int32V;
import klattice.wire.msg.type.StrV;

import java.util.ArrayList;
import java.util.Collection;

public record RowDescription(Collection<Field> fields) implements Message {
    @Override
    public char command() {
        return PgsqlServerCommandType.RowDescription.id;
    }

    public short fieldsNo() {
        return (short) fields().size();
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return () -> {
            var messageFields = new ArrayList<MessageField<?>>();
            messageFields.add(new MessageField<>("fieldsNo", Int16V::new));
            for (int i = 0; i < fieldsNo(); i++) {
                messageFields.add(new MessageField<>("name" + i, StrV::new));
                messageFields.add(new MessageField<>("fieldOid" + i, Int32V::new));
                messageFields.add(new MessageField<>("colNo" + i, Int16V::new));
                messageFields.add(new MessageField<>("dataTypeOid" + i, Int32V::new));
                messageFields.add(new MessageField<>("dataTypeLen" + i, Int16V::new));
                messageFields.add(new MessageField<>("typeModifier" + i, Int32V::new));
                messageFields.add(new MessageField<>("formatCode" + i, Int16V::new));
            }
            return new PgsqlPayload(command(), messageFields);
        };
    }

    @Override
    public Object[] values() {
        var values = new ArrayList<>();
        values.add(fieldsNo());
        for (Field field : fields()) {
            values.add(field.name());
            values.add(field.oid());
            values.add(field.colNo());
            values.add(field.dataTypeOid());
            values.add(field.dataTypeLen());
            values.add(field.modifier());
            values.add(field.fieldType().ordinal());
        }
        return values.toArray();
    }

    public record Field(String name, int oid, short colNo, int dataTypeOid, short dataTypeLen, int modifier, RowFieldType fieldType) {}
}
