package klattice.plan;

import io.substrait.proto.Plan;
import jakarta.enterprise.context.Dependent;

@Dependent
public class Enhance {
    public Plan improve(Plan source) {
        var plan = Plan.newBuilder();
        plan.mergeFrom(source);
        return plan.build();
    }
}
