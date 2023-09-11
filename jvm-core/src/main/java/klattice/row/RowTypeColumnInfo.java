package klattice.row;

import java.util.Optional;

public record RowTypeColumnInfo(String colName, RowValueType rowValueType, Optional<RowConverter> rowConverterOpt) {
}
