package klattice.schema;

public record SchemaEntity(String name, int id, String schema, SchemaType schemaType) {
    public enum SchemaType {
        AVRO, PROTOBUF, JSON
    }
}
