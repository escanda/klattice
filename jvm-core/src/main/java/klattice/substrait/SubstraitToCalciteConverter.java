package klattice.substrait;

import io.substrait.isthmus.SubstraitRelNodeConverter;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.relation.VirtualTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;

import static klattice.substrait.CalciteToSubstraitConverter.EXTENSION_COLLECTION;
import static klattice.substrait.Shared.additionalSignatures;

public interface SubstraitToCalciteConverter {
    static List<RelNode> getRelRoots(io.substrait.plan.Plan relPlan) {
        var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var converter = new SubstraitToCalcite(EXTENSION_COLLECTION, typeFactory, TypeConverter.DEFAULT) {
            @Override
            protected SubstraitRelNodeConverter createSubstraitRelNodeConverter(RelBuilder relBuilder) {
                return new SubstraitRelNodeConverter(
                        typeFactory,
                        relBuilder,
                        new ScalarFunctionConverter(EXTENSION_COLLECTION.scalarFunctions(), additionalSignatures, typeFactory, TypeConverter.DEFAULT),
                        new AggregateFunctionConverter(EXTENSION_COLLECTION.aggregateFunctions(), typeFactory),
                        TypeConverter.DEFAULT
                ) {
                    @Override
                    public RelNode visit(VirtualTableScan virtualTableScan) throws RuntimeException {
                        var recordRelDataType = typeConverter.toCalcite(typeFactory, virtualTableScan.getRecordType());
                        return relBuilder.values(recordRelDataType).build();
                    }
                };
            }
        };
        return relPlan.getRoots().stream().map(root -> converter.convert(root.getInput())).toList();
    }
}
