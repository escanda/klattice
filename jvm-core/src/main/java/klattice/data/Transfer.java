package klattice.data;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public record Transfer(Pull origin, Push destination) {
    public CompletableFuture<Transfered> perform(KafkaFetcher kafkaFetcher) {
        var tableQualifiedName = origin.tableName();
        return CompletableFuture.supplyAsync(() -> {
            var tableData = kafkaFetcher.sinceBeginning(tableQualifiedName);
            return new Transfered(this, 10L, tableData);
        });
    }
}
