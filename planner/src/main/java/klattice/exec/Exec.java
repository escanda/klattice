package klattice.exec;

import com.google.protobuf.ByteString;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import klattice.duckdb.DuckDbService;
import klattice.msg.Batch;
import klattice.msg.Column;
import klattice.msg.Row;

import java.nio.charset.StandardCharsets;
import java.util.List;

@ApplicationScoped
public class Exec {
    private final DuckDbService duckDbService;

    @Inject
    public Exec(DuckDbService duckDbService) {
        this.duckDbService = duckDbService;
    }

    public Batch execute(List<Column> columns, String sql) {
        var iterableResult = duckDbService.execSql(sql);
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
