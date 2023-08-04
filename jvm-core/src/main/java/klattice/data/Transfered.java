package klattice.data;

import klattice.msg.Batch;

import java.util.stream.Stream;

public record Transfered(Transfer owner,
                         long rowCount,
                         Stream<Batch> batches) {
}
