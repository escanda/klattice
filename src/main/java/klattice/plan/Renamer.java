package klattice.plan;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlShuttle;

import java.util.ArrayDeque;
import java.util.Objects;

public class Renamer extends SqlShuttle {
    private final ArrayDeque<Status> statuses = new ArrayDeque<>();

    public Renamer() {
        super();
        this.statuses.add(Status.INIT);
    }

    @Override
    public SqlNode visit(SqlCall call) {
        switch (Objects.requireNonNull(statuses.pollFirst())) {
            case INIT -> {
                if (call.getKind() == SqlKind.SELECT) {
                    statuses.add(Status.WITHIN_SELECT);
                    var sqlNode = (SqlSelect) super.visit(call);
                    statuses.pop();
                    return sqlNode;
                }
            }
            case WITHIN_SELECT -> {

            }
            case WITHIN_FROM -> {
            }
        }
        return super.visit(call);
    }

    enum Status {
        INIT, WITHIN_SELECT, WITHIN_FROM
    }
}
