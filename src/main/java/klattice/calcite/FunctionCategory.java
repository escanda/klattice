package klattice.calcite;

import jakarta.annotation.Nullable;

public enum FunctionCategory {
    EQUIVALENCE(null), MAGIC("kind");
    public final String queryField;

    FunctionCategory(@Nullable String queryField) {
        this.queryField = queryField;
    }
}
