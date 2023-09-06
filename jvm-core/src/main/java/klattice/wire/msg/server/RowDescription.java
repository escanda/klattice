package klattice.wire.msg.server;

import klattice.wire.hnd.RowFieldType;
import klattice.wire.msg.*;
import klattice.wire.msg.type.Int16V;
import klattice.wire.msg.type.Int32V;
import klattice.wire.msg.type.StrV;

import java.util.ArrayList;
import java.util.Collection;

public record RowDescription(int rowNo,
                             short fieldsNo,
                             Collection<Field> fields) implements Message {
    @Override
    public char command() {
        return PgsqlServerCommandType.RowDescription.id;
    }

    @Override
    public PgsqlPayloadProvider provider() {
        return () -> {
            Collection<MessageField<?>> messageFields = new ArrayList<>();
            messageFields.add(new MessageField<>("rowNo", Int32V::new));
            messageFields.add(new MessageField<>("fieldsNo", Int16V::new));
            for (int i = 0; i < fieldsNo(); i++) {
                messageFields.add(new MessageField<>("name" + i, StrV::new));
                messageFields.add(new MessageField<>("fieldOid" + i, Int32V::new));
                messageFields.add(new MessageField<>("attrNo" + i, Int16V::new));
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
        values.add(rowNo());
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
