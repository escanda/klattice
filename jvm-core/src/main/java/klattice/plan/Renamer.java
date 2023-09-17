package klattice.plan;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlShuttle;

public class Renamer extends SqlShuttle {
    @Override
    public SqlNode visit(SqlCall call) {
        if (call.getKind() == SqlKind.SELECT) {
            var select = (SqlSelect) call;
            // TODO: switch from clause literal. use stack.
        }
        return super.visit(call);
    }
}
