package klattice.row;

@FunctionalInterface
public interface RowConverter {
    Object convert(Object instance);
}
