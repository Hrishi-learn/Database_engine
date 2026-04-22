package server;

public class Session {
    private String transactionId = "-1";
    private boolean inTransaction = false;

    public void setInTransaction(boolean inTransaction) {
        this.inTransaction = inTransaction;
    }
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }
    public String getTransactionId() {
        return transactionId;
    }
    public boolean isInTransaction() {
        return inTransaction;
    }
}
