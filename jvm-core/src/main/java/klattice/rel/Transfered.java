package klattice.rel;

import klattice.msg.Batch;

import java.util.stream.Stream;

public record Transfered(Transfer owner,
                         long rowCount,
                         Stream<Batch> batches) {
}
