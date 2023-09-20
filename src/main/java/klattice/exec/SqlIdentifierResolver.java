package klattice.exec;

import com.google.common.collect.ImmutableList;
import jakarta.enterprise.context.ApplicationScoped;
import klattice.calcite.BuiltinTables;
import klattice.msg.Environment;
import klattice.msg.Rel;
import klattice.msg.Schema;
import org.apache.calcite.sql.SqlIdentifier;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

@ApplicationScoped
public class SqlIdentifierResolver {
    @ConfigProperty(name = "quarkus.http.host")
    String host;

    @ConfigProperty(name = "quarkus.http.port")
    int port;

    public Optional<TranslatedIdRef> resolve(Environment environ, SqlIdentifier id) {
        var formatted = String.format("%s", String.join(".", id.names));
        var builtinOpt = Arrays.stream(BuiltinTables.values()).filter(builtinTable -> builtinTable.tableName.equalsIgnoreCase(formatted)).findAny();
        return builtinOpt.map(builtinTables -> new TranslatedIdRef(String.format("http://%s:%d/sys-table/", host, port) + builtinTables.tableName + ".parquet"))
                .or(() -> locateRefAtEnviron(environ, id));
    }

    private Optional<TranslatedIdRef> locateRefAtEnviron(Environment environ, SqlIdentifier id) {
        return environ.getSchemasList().stream()
                .flatMap(schema -> schema.getRelsList().stream().map(rel -> new SchemaAndRel(schema, rel)))
                .flatMap(schemaAndRel -> isEq(id, schemaAndRel) ?
                                Stream.of(new SchemaAndRelMatch(schemaAndRel.schema(), schemaAndRel.rel(), id.names)) : Stream.empty())
                .map(this::toURL)
                .findFirst();
    }

    private TranslatedIdRef toURL(SchemaAndRelMatch schemaAndRelMatch) {
        var last = schemaAndRelMatch.names.stream().reduce((a, b) -> b).orElse("");
        return new TranslatedIdRef(String.format("http://%s:%d/topic-table/", host, port) + last + ".parquet");
    }

    private static boolean isEq(SqlIdentifier id, SchemaAndRel schemaAndRel) {
        return schemaAndRel.schema().getRelName().equalsIgnoreCase(id.names.stream().findFirst().orElse(""))
                && (id.names.size() > 1 && id.names.get(1).equalsIgnoreCase(schemaAndRel.rel().getRelName()));
    }

    public record TranslatedIdRef(String url) {}

    private record SchemaAndRel(Schema schema, Rel rel) {}

    private record SchemaAndRelMatch(Schema schema, Rel rel, ImmutableList<String> names) {}
}
