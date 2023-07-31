package klattice.plan;

import io.substrait.proto.Plan;
import jakarta.enterprise.context.Dependent;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Optional;

@Dependent
public class Unify {
    public Unified unification(Collection<Plan> plans) {
        var firstPlan = plans.stream().findFirst().flatMap(plan -> Optional.of(plan.newBuilderForType().mergeFrom(plan)));
        return new Unified(firstPlan, null);
    }

    public static class Unified {
        public final Optional<Plan.Builder> planBuilder;
        public final String errorMessage;

        public Unified(Optional<Plan.Builder> planBuilder, @Nullable String errorMessage) {
            this.planBuilder = planBuilder;
            this.errorMessage = errorMessage;
        }
    }
}
