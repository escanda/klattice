package klattice.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;

public class PullKafkaRelNode extends AbstractRelNode {
    public PullKafkaRelNode(RelOptCluster cluster, RelTraitSet traitSet) {
        super(cluster, traitSet);
    }
}