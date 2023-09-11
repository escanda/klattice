package klattice.registry;

public record SchemaSubject(String subject, int id, int version, String schema, String schemaType) {
    public String schemaTypeStr() {
        return schemaType() == null ? "AVRO" : schemaType();
    }
}
