package klattice.registry;

public record SchemaEntity(String name, int id, String schema, SchemaType schemaType) {
    public enum SchemaType {
        AVRO, PROTOBUF, JSON
    }
}
