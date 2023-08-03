package klattice.data;

import jakarta.enterprise.context.Dependent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

@Dependent
public class KafkaFetcher {
    public static Map<Transfer, Transfered> fetchAll(KafkaFetcher fetcher, List<Transfer> transfers) throws IOException {
        var transferMap = new LinkedHashMap<Transfer, Transfered>();
        List<Future<Transfered>> futures = new ArrayList<>();
        for (Transfer transfer : transfers) {
            futures.add(transfer.perform(fetcher));
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
                transferMap.put(transfered.owner, transfered);
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException();
            }
        }
        return transferMap;
    }

    public TableData sinceBeginning(List<String> qualifiedName) {
        return new TableData(qualifiedName);
    }
}
