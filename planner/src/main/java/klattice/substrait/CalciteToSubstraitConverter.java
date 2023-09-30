package klattice.substrait;

import io.substrait.extension.ExtensionCollector;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.TypeConverter;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.relation.RelProtoConverter;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.IOException;

public final class CalciteToSubstraitConverter {
    public static final SimpleExtension.ExtensionCollection EXTENSION_COLLECTION;

    static {
        SimpleExtension.ExtensionCollection defaults;
        try {
            var is = CalciteToSubstraitConverter.class.getResourceAsStream("/postgres_scalar_functions.yml");
            var scalarFunctions = SimpleExtension.load("postgres_scalar_functions.yml", is);
            defaults = SimpleExtension.loadDefaults()
                    .merge(scalarFunctions);
        } catch (IOException e) {
            throw new RuntimeException("Failure while loading defaults.", e);
        }

        EXTENSION_COLLECTION = defaults;
    }

    public static Plan.Builder getPlan(CalciteSchema rootSchema, RelDataTypeFactory relDataTypeFactory, RelRoot relRoot) {
        var plan = Plan.newBuilder();
        ExtensionCollector functionCollector = new ExtensionCollector();
        var relProtoConverter = new RelProtoConverter(functionCollector);
        var input = Shared.createSubstraitRelVisitor(relDataTypeFactory)
                .apply(relRoot.rel)
                .accept(relProtoConverter);
        var names = TypeConverter.DEFAULT
                .toNamedStruct(relRoot.validatedRowType)
                .names();
        plan.addRelations(
                PlanRel.newBuilder()
                        .setRoot(
                                io.substrait.proto.RelRoot.newBuilder()
                                        .setInput(input)
                                        .addAllNames(names)));
        functionCollector.addExtensionsToPlan(plan);
        return plan;
    }

    public static RelDataType asType(io.substrait.proto.Type typing) {
        final RelDataType[] types = new RelDataType[]{null};
        var typeSystem = RelDataTypeSystem.DEFAULT;
        switch (typing.getKindCase()) {
            case BOOL -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.BOOLEAN);
            case I8, I16, I32, I64 -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.INTEGER);
            case FP32 -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.FLOAT);
            case FP64 -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.DOUBLE);
            case STRING, FIXED_CHAR, VARCHAR -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.VARCHAR);
            case BINARY, UUID, FIXED_BINARY -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.BINARY);
            case TIMESTAMP -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.TIMESTAMP);
            case DATE -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.DATE);
            case TIME -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.TIME);
            case INTERVAL_YEAR -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.INTERVAL_YEAR);
            case INTERVAL_DAY -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.INTERVAL_DAY);
            case TIMESTAMP_TZ -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
            case DECIMAL -> types[0] = new BasicSqlType(typeSystem, SqlTypeName.DECIMAL);
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
