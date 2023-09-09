package klattice.substrait;

import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.isthmus.TypeConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import java.util.List;

public interface SubstraitToCalciteConverter {
    static List<RelNode> getRelRoots(io.substrait.plan.Plan relPlan) {
        var converter = new SubstraitToCalcite(
                CalciteToSubstraitConverter.EXTENSION_COLLECTION,
                new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT),
            TypeConverter.DEFAULT
        );
        return relPlan.getRoots().stream().map(root -> converter.convert(root.getInput())).toList();
    }
}
