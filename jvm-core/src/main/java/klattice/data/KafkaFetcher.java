package klattice.data;

import jakarta.enterprise.context.Dependent;
import klattice.msg.Batch;
import klattice.msg.Environment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Stream;

@Dependent
public class KafkaFetcher {
    public static Map<Transfer, Transfered> fetchAll(Environment environ, KafkaFetcher fetcher, List<Transfer> transfers) throws IOException {
        var transferMap = new LinkedHashMap<Transfer, Transfered>();
        List<Future<Transfered>> futures = new ArrayList<>();
        for (Transfer transfer : transfers) {
            futures.add(transfer.perform(environ, fetcher));
        }
        var alledFuture = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            alledFuture.get(1, TimeUnit.MINUTES);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new IOException("Cannot fetch all in one second", e);
        }
        for (var future : futures) {
            try {
                var transfered = future.get();
                transferMap.put(transfered.owner(), transfered);
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException();
            }
        }
        return transferMap;
    }

    public Stream<Batch> sinceBeginning(Environment environ, List<String> qualifiedName) {
        return Stream.empty();
    }
}
