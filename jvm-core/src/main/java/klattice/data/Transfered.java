package klattice.data;

public class Transfered {
    public final Transfer owner;
    public final long rowCount;
    public final TableData tableData;

    public Transfered(Transfer transfer, long rowCount, TableData tableData) {
        this.owner = transfer;
        this.rowCount = rowCount;
        this.tableData = tableData;
    }
}
