package klattice.substrait;

import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import klattice.calcite.FunctionDefs;
import klattice.schema.SchemaHolder;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.ReflectiveConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.util.Arrays;
import java.util.List;

import static klattice.substrait.CalciteToSubstraitConverter.EXTENSION_COLLECTION;

public interface Shared {
    RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    List<FunctionMappings.Sig> additionalSignatures = Arrays.stream(FunctionDefs.values())
            .map(functionDefs -> FunctionMappings.s(functionDefs.operator))
            .toList();

    static SubstraitRelVisitor createSubstraitRelVisitor(RelDataTypeFactory relDataTypeFactory) {
        return createSubstraitRelVisitor(relDataTypeFactory, additionalSignatures);
    }

    static SubstraitRelVisitor createSubstraitRelVisitor(RelDataTypeFactory relDataTypeFactory, List<FunctionMappings.Sig> additionalSignatures) {
        return new SubstraitRelVisitor(
                relDataTypeFactory,
                new MyScalarFunctionConverter(EXTENSION_COLLECTION.scalarFunctions(), additionalSignatures, relDataTypeFactory, TypeConverter.DEFAULT),
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
                new MyScalarFunctionConverter(EXTENSION_COLLECTION.scalarFunctions(), additionalSignatures, relDataTypeFactory, TypeConverter.DEFAULT),
                new AggregateFunctionConverter(EXTENSION_COLLECTION.aggregateFunctions(), relDataTypeFactory),
                TypeConverter.DEFAULT
        );
    }

    static FrameworkConfig framework(SchemaHolder schemaHolder) {
        return Frameworks.newConfigBuilder()
                .parserConfig(sqlParserConfig())
                .defaultSchema(schemaHolder.getCatalog().getRootSchema().plus())
                .operatorTable(schemaHolder.getSqlOperatorTable())
                .build();
    }

    static SqlToRelConverter createSqlToRelConverter(SchemaHolder schemaHolder) {
        return new SqlToRelConverter(
                ViewExpanders.simpleContext(schemaHolder.getRelOptCluster()),
                createSqlValidator(schemaHolder),
                schemaHolder.getCatalog(),
                schemaHolder.getRelOptCluster(),
                new ReflectiveConvertletTable(),
                SqlToRelConverter.CONFIG.withRelBuilderFactory((cluster, schema) -> RelBuilder
                        .create(Frameworks.newConfigBuilder()
                                .defaultSchema(schemaHolder.getCatalog().getRootSchema().plus())
                                .build())
                )
        );
    }

    static SqlAdvisorValidator createSqlValidator(SchemaHolder schemaHolder) {
        return new SqlAdvisorValidator(schemaHolder.getSqlOperatorTable(), schemaHolder.getCatalog(), schemaHolder.getTypeFactory(), SqlValidator.Config.DEFAULT);
    }

    static SqlParser.Config sqlParserConfig() {
        return SqlParser.configBuilder()
                .setConformance(SqlConformanceEnum.DEFAULT)
                .setParserFactory(SqlParserImpl.FACTORY)
                .setUnquotedCasing(Casing.TO_UPPER)
                .setQuoting(Quoting.DOUBLE_QUOTE)
                .build();
    }
}
