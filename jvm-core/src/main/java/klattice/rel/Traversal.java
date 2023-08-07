package klattice.rel;

import com.google.common.collect.Streams;

import java.util.stream.Stream;

public final class Traversal {
    private Traversal() {}

    public static Stream<Operand> depthFirst(Operand root) {
        return Streams.concat(Stream.of(root), root.children().stream().flatMap(Traversal::depthFirst));
    }
}
