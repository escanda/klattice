package klattice.substrait;

import io.substrait.expression.Expression;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.SubstraitToCalcite;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.ExpressionRexConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.proto.Plan;
import klattice.calcite.DuckDbDialect;
import klattice.calcite.FunctionShapes;
import klattice.calcite.SchemaHolder;
import klattice.exec.SqlIdentifierResolver;
import klattice.msg.Environment;
import klattice.msg.SqlStatements;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.prepare.CalciteSqlValidator;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.ReflectiveConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static klattice.substrait.CalciteToSubstraitConverter.EXTENSION_COLLECTION;

public interface Shared {
    RelDataTypeFactory relDataTypeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    List<FunctionMappings.Sig> additionalSignatures = Arrays.stream(FunctionShapes.values())
            .map(functionShapes -> FunctionMappings.s(functionShapes.operator))
            .toList();

    static SubstraitRelVisitor createSubstraitRelVisitor(RelDataTypeFactory relDataTypeFactory) {
        return createSubstraitRelVisitor(relDataTypeFactory, additionalSignatures);
    }

    static SubstraitRelVisitor createSubstraitRelVisitor(RelDataTypeFactory relDataTypeFactory, List<FunctionMappings.Sig> additionalSignatures) {
        return new SubstraitRelVisitor(
                relDataTypeFactory,
                new MyScalarFunctionConverter(EXTENSION_COLLECTION.scalarFunctions(), additionalSignatures, relDataTypeFactory, TypeConverter.DEFAULT),
                new AggregateFunctionConverter(EXTENSION_COLLECTION.aggregateFunctions(), relDataTypeFactory),
                new WindowFunctionConverter(EXTENSION_COLLECTION.windowFunctions(), relDataTypeFactory),
                TypeConverter.DEFAULT,
                ImmutableFeatureBoard.builder().build()
        );
    }

    static ExpressionRexConverter createExpressionRexConverter(RelDataTypeFactory relDataTypeFactory) {
        return new ExpressionRexConverter(
                relDataTypeFactory,
                new MyScalarFunctionConverter(EXTENSION_COLLECTION.scalarFunctions(), additionalSignatures, relDataTypeFactory, TypeConverter.DEFAULT),
                new WindowFunctionConverter(EXTENSION_COLLECTION.windowFunctions(), relDataTypeFactory),
                TypeConverter.DEFAULT
        ) {

            private final SubstraitToCalcite substraitToCalcite = SubstraitToCalciteConverter.createSubstraitToCalcite(typeFactory);

            @Override
            public RexNode visit(Expression.ScalarSubquery expr) throws RuntimeException {
                var relNode = substraitToCalcite.convert(expr.input());
                return RexSubQuery.scalar(relNode);
            }
        };
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
                viewExpander(schemaHolder),
                createSqlValidator(schemaHolder),
                schemaHolder.getCatalog(),
                schemaHolder.getRelOptCluster(),
                new ReflectiveConvertletTable(),
                SqlToRelConverter.CONFIG
        );
    }

    static SqlValidator createSqlValidator(SchemaHolder schemaHolder) {
        return new CalciteSqlValidator(schemaHolder.getSqlOperatorTable(), schemaHolder.getCatalog(), schemaHolder.getTypeFactory(), SqlValidator.Config.DEFAULT);
    }

    static SqlParser.Config sqlParserConfig() {
        var config = SqlParser.config();
        config = DuckDbDialect.INSTANCE.configureParser(config);
        return config.withCaseSensitive(false);
    }

    static SqlString toSql(SqlIdentifierResolver resolver, Environment environ, Plan plan) {
        var reifiedPlan = new ProtoPlanConverter(EXTENSION_COLLECTION).from(plan);
        var relRoots = SubstraitToCalciteConverter.getRelRoots(reifiedPlan);
        var sqlStmts = relRoots.stream().map(relNode -> new RelToSqlConverter(DuckDbDialect.INSTANCE).visitRoot(relNode).asSelect()).toList();
        var sql = sqlStmts.stream().findFirst().orElseThrow();
        var sqlNode = sql.accept(new SqlShuttle() {
            @Override
            public @Nullable SqlNode visit(SqlIdentifier id) {
                return resolver.resolve(environ, id)
                        .map(translatedIdRef -> new SqlIdentifier(translatedIdRef.url(), id.getCollation(), id.getParserPosition()))
                        .orElseGet(() -> (SqlIdentifier) super.visit(id));
            }
        });
        return sqlNode.toSqlString(DuckDbDialect.INSTANCE);
    }

    static RelOptTable.ViewExpander viewExpander(SchemaHolder schemaHolder) {
        return ViewExpanders.simpleContext(schemaHolder.getRelOptCluster());
    }

    static List<RelRoot> parseSql(SqlIdentifierResolver resolver, Environment environ, SqlStatements sqlStatements) throws Exception {
        var schemaHolder = new SchemaHolder(environ);
        var sqlNodes = new ArrayList<SqlSelect>(sqlStatements.getSqlStatementCount());
        for (String sqlStatement : sqlStatements.getSqlStatementList()) {
            var bis = new ByteArrayInputStream(sqlStatement.getBytes(StandardCharsets.UTF_8));
            var parser = Shared.sqlParserConfig().parserFactory().getParser(new InputStreamReader(bis));
            var sqlNode = (SqlSelect) parser.parseSqlStmtEof();
            sqlNodes.add(sqlNode);
        }
        var sqlToRelConverter = new SqlToRelConverter(viewExpander(schemaHolder), schemaHolder.getSqlValidator(), schemaHolder.getCatalog(), schemaHolder.getRelOptCluster(), new ReflectiveConvertletTable(), SqlToRelConverter.CONFIG);
        return sqlNodes.stream().map(sqlNode -> (RelRoot) sqlToRelConverter.convertSelect(sqlNode, true)).toList();
    }
}
