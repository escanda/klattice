package klattice.substrait;

import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import klattice.calcite.FunctionDefs;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.Arrays;
import java.util.List;

public interface Shared {
    List<FunctionMappings.Sig> additionalSignatures = Arrays.stream(FunctionDefs.values())
            .map(functionDefs -> FunctionMappings.s(functionDefs.operator))
            .toList();

    static SubstraitRelVisitor createSubstraitRelVisitor(RelDataTypeFactory relDataTypeFactory, List<FunctionMappings.Sig> additionalSignatures) {
        return new SubstraitRelVisitor(
                relDataTypeFactory,
                new ScalarFunctionConverter(CalciteToSubstraitConverter.EXTENSION_COLLECTION.scalarFunctions(), additionalSignatures, relDataTypeFactory, TypeConverter.DEFAULT),
                new AggregateFunctionConverter(CalciteToSubstraitConverter.EXTENSION_COLLECTION.aggregateFunctions(), relDataTypeFactory),
                new WindowFunctionConverter(CalciteToSubstraitConverter.EXTENSION_COLLECTION.windowFunctions(), relDataTypeFactory,
                        new AggregateFunctionConverter(CalciteToSubstraitConverter.EXTENSION_COLLECTION.aggregateFunctions(), relDataTypeFactory),
                        TypeConverter.DEFAULT
                ),
                TypeConverter.DEFAULT,
                ImmutableFeatureBoard.builder().build()
        );
    }
}
