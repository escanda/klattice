package klattice.exec;

import com.google.protobuf.ByteString;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import klattice.duckdb.DuckDbService;
import klattice.msg.Batch;
import klattice.msg.Row;

import java.nio.charset.StandardCharsets;

@ApplicationScoped
public class Exec {
    private final DuckDbService duckDbService;

    @Inject
    public Exec(DuckDbService duckDbService) {
        this.duckDbService = duckDbService;
    }

    public Batch execute(String sql) {
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
