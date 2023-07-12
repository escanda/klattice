package klattice.query;

import io.quarkus.arc.log.LoggerName;
import io.quarkus.grpc.GrpcService;
import io.smallrye.mutiny.Uni;
import io.substrait.extension.ExtensionCollector;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.RelProtoConverter;
import jakarta.inject.Inject;
import klattice.msg.*;
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
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static klattice.query.Prepare.EXTENSION_COLLECTION;
import static klattice.query.Prepare.asType;

@GrpcService
public class QueryServiceGrpc implements Query {
    @LoggerName("QueryServiceGrpc")
    Logger logger;

    @Override
    public Uni<PreparedQuery> prepare(QueryDescriptor request) {
        try {
            var parser = SqlParser.create(request.getQuery());
            var sql = parser.parseQuery();;
            var rootSchema = LookupCalciteSchema.createRootSchema(false);
            for (SchemaDescriptor schemaSourceDetails : request.getSourcesList()) {
                CalciteSchema schemaPlus = CalciteSchema.createRootSchema(false);
                for (RelDescriptor projection : schemaSourceDetails.getProjectionsList()) {
                    List<RelDataTypeField> typeList = new ArrayList<>();
                    int i = 0;
                    for (String columnName : projection.getColumnNameList()) {
                        typeList.add(new RelDataTypeFieldImpl(columnName, i, asType(projection.getTyping(i))));
                        i++;
                    }
                    var table = new ListTransientTable(projection.getRelName(), new RelRecordType(StructKind.FULLY_QUALIFIED, typeList));
                    schemaPlus.add(projection.getRelName(), table);
                }
                rootSchema.add(schemaSourceDetails.getRelName(), schemaPlus.plus());
            }

            JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

            var program = HepProgram.builder().build();
            var planner = new HepPlanner(program);
            RelOptCluster relOptCluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));
            relOptCluster.setMetadataQuerySupplier(
                    () -> {
                        ProxyingMetadataHandlerProvider handler =
                                new ProxyingMetadataHandlerProvider(DefaultRelMetadataProvider.INSTANCE);
                        return new RelMetadataQuery(handler);
                    });

            var props = new Properties();
            props.put("caseSensitive", Boolean.FALSE);

            CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                    rootSchema,
                    request.getSourcesList().stream()
                            .findFirst()
                            .map(schemaDescriptor -> List.of(schemaDescriptor.getRelName())).orElse(Collections.emptyList()),
                    typeFactory,
                    new CalciteConnectionConfigImpl(props)
            );

            var operatorTable = new SqlStdOperatorTable();
            var calciteSqlValidator = new CalciteSqlValidator(operatorTable, catalogReader, typeFactory, SqlValidator.Config.DEFAULT);
            var plan = Plan.newBuilder();
            ExtensionCollector functionCollector = new ExtensionCollector();
            var relProtoConverter = new RelProtoConverter(functionCollector);
            SqlToRelConverter converter =
                    new SqlToRelConverter(
                            null,
                            calciteSqlValidator,
                            catalogReader,
                            relOptCluster,
                            StandardConvertletTable.INSTANCE,
                            SqlToRelConverter.config());
            var relRoot = converter.convertQuery(sql, false, true);
            planner.setRoot(relRoot.rel);
            relRoot = relRoot.withRel(planner.findBestExp());
            plan.addRelations(
                    PlanRel.newBuilder()
                            .setRoot(
                                    io.substrait.proto.RelRoot.newBuilder()
                                            .setInput(
                                                    SubstraitRelVisitor.convert(
                                                                    relRoot, EXTENSION_COLLECTION, ImmutableFeatureBoard.builder().build())
                                                            .accept(relProtoConverter))
                                            .addAllNames(
                                                    TypeConverter.DEFAULT
                                                            .toNamedStruct(relRoot.validatedRowType)
                                                            .names())));
            functionCollector.addExtensionsToPlan(plan);
            logger.info(plan);
            return Uni.createFrom().item(PreparedQuery.newBuilder().setPlan(plan).build());
        } catch (Exception e) {
            logger.error(e);
            var pq = PreparedQuery.newBuilder().setDiagnostics(QueryDiagnostics.newBuilder().setErrorMessage(e.getMessage()).build());
            return Uni.createFrom().item(pq.build());
        }
    }
}
