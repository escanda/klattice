package klattice.query;

import com.google.common.annotations.VisibleForTesting;
import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.FeatureBoard;
import io.substrait.isthmus.ImmutableFeatureBoard;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.RelProtoConverter;
import jakarta.enterprise.context.Dependent;
import klattice.msg.RelDescriptor;
import klattice.msg.SchemaDescriptor;
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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.calcite.sql.validate.SqlConformance.PRAGMATIC_2003;

@Dependent
public class Prepare {
    public static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION;

    static {
        SimpleExtension.ExtensionCollection defaults;
        try {
            defaults = SimpleExtension.loadDefaults();
        } catch (IOException e) {
            throw new RuntimeException("Failure while loading defaults.", e);
        }

        EXTENSION_COLLECTION = defaults;
    }

    public static RelDataType asType(io.substrait.proto.Type typing) {
        final RelDataType[] types = new RelDataType[]{null};
        var typeSystem = RelDataTypeSystem.DEFAULT;
        switch (typing.getKindCase()) {
            case BOOL -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.BOOLEAN);
            }
            case I8 -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.INTEGER);
            }
            case I16 -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.INTEGER);
            }
            case I32 -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.INTEGER);
            }
            case I64 -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.INTEGER);
            }
            case FP32 -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.FLOAT);
            }
            case FP64 -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.DOUBLE);
            }
            case STRING -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.VARCHAR);
            }
            case BINARY -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.BINARY);
            }
            case TIMESTAMP -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.TIMESTAMP);
            }
            case DATE -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.DATE);
            }
            case TIME -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.TIME);
            }
            case INTERVAL_YEAR -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.INTERVAL_YEAR);
            }
            case INTERVAL_DAY -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.INTERVAL_DAY);
            }
            case TIMESTAMP_TZ -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
            }
            case UUID -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.BINARY);
            }
            case FIXED_CHAR -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.VARCHAR);
            }
            case VARCHAR -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.VARCHAR);
            }
            case FIXED_BINARY -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.BINARY);
            }
            case DECIMAL -> {
                types[0] = new BasicSqlType(typeSystem, SqlTypeName.DECIMAL);
            }
            case STRUCT -> {
            }
            case LIST -> {
            }
            case MAP -> {
            }
            case USER_DEFINED -> {
            }
            case USER_DEFINED_TYPE_REFERENCE -> {
            }
            case KIND_NOT_SET -> {
            }
            default -> {
            }
        }
        return types[0];
    }
}
