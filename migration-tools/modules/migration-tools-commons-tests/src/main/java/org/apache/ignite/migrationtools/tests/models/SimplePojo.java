/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.tests.models;

import java.math.BigDecimal;
import java.util.Objects;

/** Simple Pojo. */
public class SimplePojo {

    private String name;

    private int amount;

    private BigDecimal decimalAmount;

    private SimplePojo() {
        // Default constructor
        // Intentionally left blank.
    }

    /** Constructor. */
    public SimplePojo(String name, int amount, BigDecimal decimalAmount) {
        this.name = name;
        this.amount = amount;
        this.decimalAmount = decimalAmount;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public BigDecimal getDecimalAmount() {
        return decimalAmount;
    }

    public void setDecimalAmount(BigDecimal decimalAmount) {
        this.decimalAmount = decimalAmount;
    }

    @Override public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimplePojo pojo = (SimplePojo) o;
        return amount == pojo.amount && Objects.equals(name, pojo.name) && Objects.equals(decimalAmount, pojo.decimalAmount);
    }

    @Override public int hashCode() {
        return Objects.hash(name, amount, decimalAmount);
    }
}
