package klattice.data;

public class Transfer {
    private final Pull origin;
    private final Push destination;

    public Transfer(Pull origin, Push destination) {
        this.origin = origin;
        this.destination = destination;
    }

    public void perform() {
    }
}
