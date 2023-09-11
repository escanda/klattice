package klattice.store;

import io.substrait.type.Type;

import java.util.Collection;

public record TypeAndName(TypeKind kind, String name) {
    sealed interface TypeKind {}
    public record TypeTerminal(String name, Type actualType) implements TypeKind {}
    public record TypeLeaf(Collection<TypeKind> nested) implements TypeKind {}
}
