package klattice.schema;

import klattice.msg.Column;
import klattice.msg.Environment;
import klattice.msg.Schema;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.jdbc.LookupCalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.*;

import static klattice.plan.Converter.asType;

public class SchemaFactory {
    private final CalciteCatalogReader catalog;
    private final RelOptCluster relOptCluster;
    private final JavaTypeFactory typeFactory;
    private final SqlStdOperatorTable operatorTable;
    private final CalciteSqlValidator calciteSqlValidator;

    public SchemaFactory(Environment environment) {
        operatorTable = new SqlStdOperatorTable();
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

        typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

        var program = HepProgram.builder().build();
        var planner = new HepPlanner(program);
        relOptCluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
        relOptCluster.setMetadataQuerySupplier(() -> {
            ProxyingMetadataHandlerProvider handler = new ProxyingMetadataHandlerProvider(DefaultRelMetadataProvider.INSTANCE);
            return new RelMetadataQuery(handler);
        });

        var props = new Properties();
        props.put("caseSensitive", Boolean.FALSE);
        this.catalog = new CalciteCatalogReader(
                rootSchema,
                environment.getSchemasList().stream()
                        .findFirst()
                        .map(schemaDescriptor -> List.of(schemaDescriptor.getRelName())).orElse(Collections.emptyList()),
                typeFactory,
                new CalciteConnectionConfigImpl(props)
        );
        this.calciteSqlValidator = new CalciteSqlValidator(operatorTable, getCatalog(), getTypeFactory(), SqlValidator.Config.DEFAULT);
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
}
