package klattice.plan.rule;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.plan.ProtoPlanConverter;
import klattice.calcite.DuckDbDialect;
import klattice.calcite.SchemaHolder;
import klattice.grpc.QueryService;
import klattice.msg.Environment;
import klattice.msg.QueryDescriptor;
import klattice.plan.Optimizer;
import klattice.substrait.SubstraitToCalciteConverter;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static klattice.substrait.CalciteToSubstraitConverter.EXTENSION_COLLECTION;

@QuarkusTest
public class MagicValuesReplaceRuleTest {
    @GrpcClient
    QueryService query;

    private Optimizer optimizer;
    private Environment environment;

    @BeforeEach
    void setupRePlanner() {
        this.environment = Environment.newBuilder().build();
        this.optimizer = new Optimizer(new SchemaHolder(environment));
    }

    @Test
    public void test_versionInvocationReplacement() {
        runQuery("SELECT version()");
    }

    private void runQuery(String query) {
        var inflated = this.query.inflate(QueryDescriptor.newBuilder().setQuery(query).setEnviron(environment).build())
                .await()
                .atMost(Duration.ofMinutes(1));
        var relPlan = new ProtoPlanConverter(EXTENSION_COLLECTION).from(inflated.getPlan().getPlan());
        var relRoots = SubstraitToCalciteConverter.getRelRoots(relPlan);
        var relNodes = optimizer.relnodes(relRoots);
        System.out.println(relNodes);
        var result = new RelToSqlConverter(DuckDbDialect.INSTANCE).visitRoot(relNodes.get(0));
        var selectNode = result.asSelect();
        System.out.println(selectNode);
    }
}
