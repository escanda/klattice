package klattice.substrait;

import io.substrait.expression.Expression;
import io.substrait.isthmus.SubstraitRelNodeConverter;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.relation.VirtualTableScan;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
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
                        var tupleList = new ArrayList<List<RexLiteral>>();
                        var rows = virtualTableScan.getRows();
                        int i = 0;
                        for (Expression.StructLiteral row : rows) {
                            int j = 0;
                            for (Expression.Literal field : row.fields()) {
                                if (i == 0) tupleList.add(new ArrayList<>(rows.size()));
                                tupleList.get(j).add((RexLiteral) field.accept(Shared.createExpressionRexConverter(typeFactory)));
                            }
                            i++;
                        }
                        return relBuilder.values(tupleList, recordRelDataType).build();
                    }
                };
            }
        };
        return relPlan.getRoots().stream().map(root -> converter.convert(root.getInput())).toList();
    }
}
