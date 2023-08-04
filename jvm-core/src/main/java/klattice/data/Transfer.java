package klattice.data;

import klattice.msg.Environment;

import java.util.concurrent.CompletableFuture;

public record Transfer(Pull origin, Push destination) {
    public CompletableFuture<Transfered> perform(Environment environ, KafkaFetcher kafkaFetcher) {
        var tableQualifiedName = origin.tableName();
        return CompletableFuture.supplyAsync(() -> {
            var batches = kafkaFetcher.sinceBeginning(environ, tableQualifiedName);
            return new Transfered(this, 10L, batches);
        });
    }
}
