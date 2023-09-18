package klattice.endpoint;

import java.util.regex.Pattern;

public interface Regex {
    Pattern RANGE_PAT = Pattern.compile(".*?bytes=(\\d+)-(\\d+)$");
}
