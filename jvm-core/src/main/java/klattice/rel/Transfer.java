package klattice.rel;

import klattice.msg.Rel;
import klattice.msg.Schema;

public record Transfer(Pull origin, Push destination, Schema schema, Rel rel) {
}
