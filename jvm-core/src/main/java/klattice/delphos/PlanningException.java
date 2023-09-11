package klattice.delphos;

import klattice.msg.PlanDiagnostics;

public class PlanningException extends Exception {
    private final PlanDiagnostics diagnostics;

    public PlanningException(String errorMessage, PlanDiagnostics diagnostics) {
        super(errorMessage);
        this.diagnostics = diagnostics;
    }

    public PlanDiagnostics getDiagnostics() {
        return diagnostics;
    }

    public static PlanningException from(PlanDiagnostics diagnostics) {
        return new PlanningException(diagnostics.getErrorMessage(), diagnostics);
    }
}
