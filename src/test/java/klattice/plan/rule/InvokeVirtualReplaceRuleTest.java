package klattice.plan.rule;

import io.quarkus.grpc.GrpcClient;
import io.quarkus.test.junit.QuarkusTest;
import io.substrait.plan.ProtoPlanConverter;
import klattice.msg.Environment;
import klattice.msg.QueryDescriptor;
import klattice.plan.RePlanner;
import klattice.query.Query;
import klattice.schema.SchemaHolder;
import klattice.substrait.SubstraitToCalciteConverter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static klattice.substrait.CalciteToSubstraitConverter.EXTENSION_COLLECTION;

@QuarkusTest
public class InvokeVirtualReplaceRuleTest {
    @GrpcClient
    Query query;

    private RePlanner rePlanner;
    private Environment environment;

    @BeforeEach
    void setupRePlanner() {
        this.environment = Environment.newBuilder().build();
        this.rePlanner = new RePlanner(new SchemaHolder(new SqlStdOperatorTable(), environment));
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
        var relNodes = rePlanner.optimizeRelNodes(relRoots);
        System.out.println(relNodes);
    }
}