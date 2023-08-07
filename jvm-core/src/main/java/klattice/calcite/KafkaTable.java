package klattice.calcite;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class KafkaTable implements TranslatableTable {
    private final String bootstrap;
    private final String topicName;

    public KafkaTable(String bootstrap, String topicName) {
        this.bootstrap = bootstrap;
        this.topicName = topicName;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new KafkaTableScan(context.getCluster(), context.getCluster().traitSet(), bootstrap, topicName);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        var schema = KafkaUtil.schemaRegistryForTopic(bootstrap, topicName);
        return typeFactory.createStructType(StructKind.PEEK_FIELDS, );
    }

    @Override
    public Statistic getStatistic() {
        Long total = KafkaUtil.countTopic(bootstrap, topicName);
        return new Statistic() {
            @Override
            public Double getRowCount() {
                return total.doubleValue();
            }
        };
    }

    @Override
    public Schema.TableType getJdbcTableType() {
        return Schema.TableType.FOREIGN_TABLE;
    }

    @Override
    public boolean isRolledUp(String column) {
        return false;
    }

    @Override
    public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call, @Nullable SqlNode parent, @Nullable CalciteConnectionConfig config) {
        return false;
    }
}
