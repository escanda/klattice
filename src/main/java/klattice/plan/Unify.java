package klattice.plan;

import io.substrait.proto.Plan;
import jakarta.enterprise.context.Dependent;

import java.util.Collection;

@Dependent
public class Unify {
    public Plan unification(Collection<Plan> plans) {
        return plans.stream().findFirst().orElse(null);
    }
}
