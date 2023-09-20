package klattice.calcite;

import klattice.msg.Column;
import klattice.msg.Environment;
import klattice.msg.Schema;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.jdbc.LookupCalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.CalciteSqlValidator;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.ProxyingMetadataHandlerProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.*;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.impl.ListTransientTable;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static klattice.substrait.CalciteToSubstraitConverter.asType;

public class SchemaHolder {
    private final Environment environment;
    private final CalciteCatalogReader catalog;
    private final RelOptCluster relOptCluster;
    private final JavaTypeFactory typeFactory;
    private final CalciteSqlValidator calciteSqlValidator;

    public SchemaHolder(Environment environment) {
        this.environment = environment;
        var rootSchema = LookupCalciteSchema.createRootSchema(false);
        for (Schema schema : environment. getSchemasList()) {
            CalciteSchema schemaPlus = CalciteSchema.createRootSchema(false);
            for (var projection : schema.getRelsList()) {
                List<RelDataTypeField> typeList = new ArrayList<>();
                int i = 0;
                for (Column col : projection.getColumnsList()) {
                    typeList.add(new RelDataTypeFieldImpl(col.getColumnName(), i, asType(col.getType())));
                    i++;
                }
                var table = new ListTransientTable(projection.getRelName(), new RelRecordType(StructKind.FULLY_QUALIFIED, typeList));
                schemaPlus.add(projection.getRelName(), table);
            }
            rootSchema.add(schema.getRelName(), schemaPlus.plus());
        }

        var enrichedSchema = enrich(rootSchema);

        typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        var program = HepProgram.builder().build();
        var planner = new HepPlanner(program);
        relOptCluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        relOptCluster.setMetadataQuerySupplier(() -> {
            var handler = new ProxyingMetadataHandlerProvider(DefaultRelMetadataProvider.INSTANCE);
            return new RelMetadataQuery(handler);
        });

        var props = new Properties();
        props.put("caseSensitive", Boolean.FALSE);
        this.catalog = new CalciteCatalogReader(
                enrichedSchema,
                environment.getSchemasList().stream()
                        .findFirst()
                        .map(Schema::getRelName)
                        .map(List::of)
                        .orElse(List.of()),
                typeFactory,
                new CalciteConnectionConfigImpl(props)
        );
        this.calciteSqlValidator = new CalciteSqlValidator(
                getSqlOperatorTable(),
                getCatalog(),
                getTypeFactory(),
                SqlValidator.Config.DEFAULT
        );
    }

    public CalciteSchema enrich(CalciteSchema schema) {
        for (BuiltinTables builtinTable : BuiltinTables.values()) {
            schema.add(builtinTable.tableName, new ListTransientTable(builtinTable.tableName, builtinTable.rowType));
        }
        return schema;
    }

    public Environment getEnvironment() {
        return environment;
    }

    public CalciteCatalogReader getCatalog() {
        return this.catalog;
    }

    public RelOptCluster getRelOptCluster() {
        return relOptCluster;
    }

    public JavaTypeFactory getTypeFactory() {
        return typeFactory;
    }

    public CalciteSqlValidator getSqlValidator() {
        return calciteSqlValidator;
    }

    public SqlOperatorTable getSqlOperatorTable() {
        return SqlOperatorTables.chain(
                SqlOperatorTables.of(getFunctionOperators()),
                new SqlStdOperatorTable()
        );
    }

    private Iterable<? extends SqlOperator> getFunctionOperators() {
        return Arrays.stream(FunctionDefs.values())
                .map(functionDef -> functionDef.operator)
                .toList();
    }

    public RelOptTable resolveTable(String tableName) {
        return getCatalog().getTable(List.of(tableName));
    }
}
