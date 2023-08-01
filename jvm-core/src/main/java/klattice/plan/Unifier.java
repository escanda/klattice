package klattice.plan;

import io.substrait.proto.Plan;
import jakarta.enterprise.context.Dependent;

import java.util.Collection;
import java.util.Optional;

@Dependent
public class Unifier {
    public Unified unify(Collection<Plan> plans) {
        var firstPlan = plans.stream().findFirst().flatMap(plan -> Optional.of(plan.newBuilderForType().mergeFrom(plan)));
        return new Unified(firstPlan, null);
    }

    public record Unified(Optional<Plan.Builder> planBuilder, String errorMessage) {
    }
}
