package klattice.row;

import java.util.Map;

public record RowTypeRoot(String rootName, Map<String, RowTypeInfo> typeInfoMap) {
}
