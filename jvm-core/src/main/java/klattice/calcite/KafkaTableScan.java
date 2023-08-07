package klattice.calcite;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;

public class KafkaTableScan extends AbstractRelNode {
    private final String bootstrap;
    private final String topicName;

    public KafkaTableScan(RelOptCluster relOptCluster, RelTraitSet traitSet, String bootstrap, String topicName) {
        super(relOptCluster, traitSet);
        this.bootstrap = bootstrap;
        this.topicName = topicName;
    }
}
