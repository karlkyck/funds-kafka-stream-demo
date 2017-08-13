package com.karlkyck.funds;

import java.math.BigDecimal;

/**
 * Created by karlkyck on 13/08/17.
 */
public class Transaction {
    public Integer id;
    public BigDecimal amount;

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Transaction that = (Transaction) o;

        return id != null ? id.equals(that.id) : that.id == null;
    }

    @Override
    public int hashCode() {
        return id != null ? id.hashCode() : 0;
    }
}
