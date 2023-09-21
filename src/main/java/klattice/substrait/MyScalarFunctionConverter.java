package klattice.substrait;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import klattice.calcite.FunctionDefs;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class MyScalarFunctionConverter extends ScalarFunctionConverter {
    public MyScalarFunctionConverter(List<SimpleExtension.ScalarFunctionVariant> functions, List<FunctionMappings.Sig> additionalSignatures, RelDataTypeFactory typeFactory, TypeConverter typeConverter) {
        super(functions, additionalSignatures, typeFactory, typeConverter);
    }

    @Override
    public Optional<Expression> convert(RexCall call, Function<RexNode, Expression> topLevelConverter) {
        return super.convert(call, topLevelConverter);
    }

    @Override
    protected ImmutableList<FunctionMappings.Sig> getSigs() {
        return super.getSigs()
                .stream()
                .filter(sig -> Arrays.stream(FunctionDefs.values()).noneMatch(functionDefs -> functionDefs.operator.equals(sig.operator())))
                .collect(ImmutableList.toImmutableList());
    }
}
