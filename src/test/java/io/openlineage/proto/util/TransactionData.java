package io.openlineage.proto.util;

public class TransactionData {
    public final String customerId;
    public final int amount;
    public final String type;
    public final String timestamp;

    public TransactionData(String customerId, int amount, String type, String timestamp) {
        this.customerId = customerId;
        this.amount = amount;
        this.type = type;
        this.timestamp = timestamp;
    }
}

