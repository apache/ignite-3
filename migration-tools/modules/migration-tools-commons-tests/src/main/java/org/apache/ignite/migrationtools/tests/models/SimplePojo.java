/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
