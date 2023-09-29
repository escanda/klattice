package klattice.calcite;

import com.google.common.collect.ImmutableList;

import java.util.Collection;

public enum FunctionCategory {
    SCHEMAS(ImmutableList.of()),
    MAGIC(ImmutableList.of(
            FunctionShapes.VERSION,
            FunctionShapes.CURRENT_USER,
            FunctionShapes.CURRENT_SCHEMA,
            FunctionShapes.CURRENT_SCHEMAS,
            FunctionShapes.CURRENT_DATABASE
    ));

    public final ImmutableList<FunctionShapes> shapes;

    FunctionCategory(Collection<FunctionShapes> functionShapes) {
        this.shapes = functionShapes.stream()
                .collect(ImmutableList.toImmutableList());
    }
}
