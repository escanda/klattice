package klattice.plan;

import jakarta.enterprise.context.Dependent;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.util.Util;

import java.util.Collection;
import java.util.List;

@Dependent
public class Partitioner {
    public Collection<RelRoot> differentiate(RelDataTypeFactory relDataTypeFactory, CalciteSchema schema, RelNode relNode) {
        Util.discard(relDataTypeFactory);
        Util.discard(schema);
        return List.of(RelRoot.of(relNode, SqlKind.SELECT));
    }
}
