package com.karlkyck.funds;

/**
 * Created by karlkyck on 13/08/17.
 */
public class ZippedTransactions {
    public Transaction source1Transaction;
    public Transaction source2Transaction;

    public ZippedTransactions(final Transaction source1Transaction, final Transaction source2Transaction) {
        this.source1Transaction = source1Transaction;
        this.source2Transaction = source2Transaction;
    }
}
