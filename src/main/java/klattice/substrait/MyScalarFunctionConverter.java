package klattice.substrait;

import com.google.common.collect.ImmutableList;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import klattice.calcite.FunctionDefs;
import org.apache.calcite.rel.type.RelDataTypeFactory;

import java.util.Arrays;
import java.util.List;

public class MyScalarFunctionConverter extends ScalarFunctionConverter {
    public MyScalarFunctionConverter(List<SimpleExtension.ScalarFunctionVariant> functions, List<FunctionMappings.Sig> additionalSignatures, RelDataTypeFactory typeFactory, TypeConverter typeConverter) {
        super(functions, additionalSignatures, typeFactory, typeConverter);
    }

    @Override
    protected ImmutableList<FunctionMappings.Sig> getSigs() {
        return super.getSigs()
                .stream()
                .filter(sig -> Arrays.stream(FunctionDefs.values()).noneMatch(functionDefs -> functionDefs.operator.equals(sig.operator())))
                .collect(ImmutableList.toImmutableList());
    }
}
