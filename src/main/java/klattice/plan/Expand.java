package klattice.plan;

import io.quarkus.arc.log.LoggerName;
import io.substrait.extension.ExtensionCollector;
import io.substrait.isthmus.SubstraitRelNodeConverter;
import io.substrait.proto.Plan;
import io.substrait.relation.ProtoRelConverter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;
import klattice.msg.Environment;
import klattice.schema.SchemaFactory;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

@Dependent
public class Expand {
    @LoggerName("Expand")
    Logger logger;

    @Inject
    Partitioner partitioner;

    public Pair<Collection<Plan>, Plan> expand(Plan source, List<Environment> environments) throws IOException {
        var plan = Plan.newBuilder();
        plan.mergeFrom(source);
        var newPlans = expandWithEnvironments(plan, environments);
        return Pair.of(newPlans, plan.build());
    }

    protected List<Plan> expandWithEnvironments(Plan.Builder plan, List<Environment> environments) throws IOException {
        var extensionCollector = new ExtensionCollector();
        var environmentList = new ArrayList<>(environments);
        var factory = new SchemaFactory(environmentList);
        final var protoRelConverter = new ProtoRelConverter(extensionCollector);
        var config = Frameworks.newConfigBuilder().defaultSchema(factory.getCatalog().getRootSchema().plus()).build();
        var typeFactory = factory.getTypeFactory();
        return plan.getRelationsList().stream().map(planRel -> protoRelConverter.from(planRel.getRoot().getInput())).flatMap(rel -> {
            if (rel == null) return Stream.empty();
            var relNodeConverter = new SubstraitRelNodeConverter(Converter.EXTENSION_COLLECTION, typeFactory, RelBuilder.create(config));
            return Stream.of(rel.accept(relNodeConverter));
        }).flatMap(relNode -> {
            var schema = Converter.getSchema(environments);
            var partitions = partitioner.differentiate(typeFactory, schema, relNode);
            return partitions.stream().map(Converter::getPlan);
        }).map(Plan.Builder::build).toList();
    }
}
