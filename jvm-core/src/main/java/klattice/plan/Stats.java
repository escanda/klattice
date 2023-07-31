package klattice.plan;

public record Stats(long records, long childrenSteps) {
    public static final Stats ZERO = of(0, 0);

    public static Stats of(long rows, long childrenSteps) {
        return new Stats(rows, childrenSteps);
    }

    public Stats merge(Stats other) {
        return Stats.of(
                this.records + (other == null ? 0L : other.records),
                this.childrenSteps + (other == null ? 0L : other.childrenSteps)
        );
    }
}
