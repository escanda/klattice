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

    public Expanded expand(Plan source, Environment environ) throws IOException {
        var plan = Plan.newBuilder();
        plan.mergeFrom(source);
        var newPlans = expandWithEnvironments(plan, environ);
        return new Expanded(newPlans, plan.build());
    }

    protected List<Plan> expandWithEnvironments(Plan.Builder plan, Environment environ) throws IOException {
        var extensionCollector = new ExtensionCollector();
        var factory = new SchemaFactory(environ);
        final var protoRelConverter = new ProtoRelConverter(extensionCollector);
        var config = Frameworks.newConfigBuilder().defaultSchema(factory.getCatalog().getRootSchema().plus()).build();
        var typeFactory = factory.getTypeFactory();
        return plan.getRelationsList().stream().map(planRel -> protoRelConverter.from(planRel.getRoot().getInput())).flatMap(rel -> {
            if (rel == null) return Stream.empty();
            var relNodeConverter = new SubstraitRelNodeConverter(Converter.EXTENSION_COLLECTION, typeFactory, RelBuilder.create(config));
            return Stream.of(rel.accept(relNodeConverter));
        }).flatMap(relNode -> {
            var partitions = partitioner.differentiate(typeFactory, new Partitioner.Schemata(environ, Converter.getSchema(environ)), relNode);
            return partitions.stream().map(Converter::getPlan);
        }).map(Plan.Builder::build).toList();
    }

    public record Expanded(Collection<Plan> plans, Plan actualPlan) {}
}
