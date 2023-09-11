package klattice.facade;

import klattice.msg.QueryDiagnostics;

public class SyntacticalException extends Exception {
    private final QueryDiagnostics diagnostics;

    public SyntacticalException(String errorMessage, QueryDiagnostics diagnostics) {
        super(errorMessage);
        this.diagnostics = diagnostics;
    }

    public QueryDiagnostics getDiagnostics() {
        return diagnostics;
    }

    public static SyntacticalException from(QueryDiagnostics diagnostics) {
        return new SyntacticalException(diagnostics.getErrorMessage(), diagnostics);
    }
}
