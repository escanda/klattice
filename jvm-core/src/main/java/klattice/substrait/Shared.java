package klattice.substrait;

import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.*;
import klattice.calcite.FunctionDefs;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.Arrays;
import java.util.List;

import static klattice.substrait.CalciteToSubstraitConverter.EXTENSION_COLLECTION;

public interface Shared {
    List<FunctionMappings.Sig> additionalSignatures = Arrays.stream(FunctionDefs.values())
            .map(functionDefs -> FunctionMappings.s(functionDefs.operator))
            .toList();

    static SubstraitRelVisitor createSubstraitRelVisitor(RelDataTypeFactory relDataTypeFactory, List<FunctionMappings.Sig> additionalSignatures) {
        return new SubstraitRelVisitor(
                relDataTypeFactory,
                new ScalarFunctionConverter(EXTENSION_COLLECTION.scalarFunctions(), additionalSignatures, relDataTypeFactory, TypeConverter.DEFAULT),
                new AggregateFunctionConverter(EXTENSION_COLLECTION.aggregateFunctions(), relDataTypeFactory),
                new WindowFunctionConverter(
                        EXTENSION_COLLECTION.windowFunctions(),
                        relDataTypeFactory,
                        new AggregateFunctionConverter(EXTENSION_COLLECTION.aggregateFunctions(), relDataTypeFactory),
                        TypeConverter.DEFAULT
                ),
                TypeConverter.DEFAULT,
                ImmutableFeatureBoard.builder().build()
        );
    }

    static ExpressionRexConverter createExpressionRexConverter(RelDataTypeFactory relDataTypeFactory) {
        return new ExpressionRexConverter(
                relDataTypeFactory,
                new ScalarFunctionConverter(EXTENSION_COLLECTION.scalarFunctions(), additionalSignatures, relDataTypeFactory, TypeConverter.DEFAULT),
                new AggregateFunctionConverter(EXTENSION_COLLECTION.aggregateFunctions(), relDataTypeFactory),
                TypeConverter.DEFAULT
        );
    }
}
