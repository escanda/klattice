package klattice.exec;

import com.google.protobuf.ByteString;
import io.substrait.proto.Plan;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import klattice.duckdb.DuckDbService;
import klattice.msg.Batch;
import klattice.msg.Environment;
import klattice.msg.Row;
import klattice.substrait.Shared;
import org.apache.calcite.sql.util.SqlString;

import java.nio.charset.StandardCharsets;

@ApplicationScoped
public class Exec {
    private final DuckDbService duckDbService;
    private final SqlIdentifierResolver sqlIdentifierResolver;

    @Inject
    public Exec(DuckDbService duckDbService, SqlIdentifierResolver sqlIdentifierResolver) {
        this.duckDbService = duckDbService;
        this.sqlIdentifierResolver = sqlIdentifierResolver;
    }

    public SqlString toSql(Environment environ, Plan plan) {
        return Shared.toSql(sqlIdentifierResolver, environ, plan);
    }

    public Batch execute(SqlString sql) {
        var iterableResult = duckDbService.execSql(sql.getSql());
        var batchBuilder = Batch.newBuilder();
        iterableResult.forEach(strings -> {
            var rowBuilder = Row.newBuilder();
            for (String string : strings) {
                rowBuilder.addFields(ByteString.copyFrom(string, StandardCharsets.UTF_8));
            }
            batchBuilder.addRows(rowBuilder);
        });
        return batchBuilder.build();
    }
}
