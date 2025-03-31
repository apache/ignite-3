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

package org.apache.ignite.example.streaming.pojo;

/**
 * POJO class that represents Account record.
 */
public class Account {
    /** Account number. */
    private int accountNumber;

    /** Name. */
    private String name;

    /** Balance. */
    private long balance;

    /** Is account active. */
    private boolean active;

    /**
     * Default constructor (required for deserialization).
     */
    @SuppressWarnings("unused")
    public Account() {
    }

    /**
     * Constructor.
     *
     * @param accountNumber Account number.
     */
    @SuppressWarnings("unused")
    public Account(int accountNumber) {
        this.accountNumber = accountNumber;
    }

    /**
     * Constructor.
     *
     * @param accountNumber Account number.
     * @param name Name.
     * @param balance Balance.
     * @param active Is account active.
     */
    public Account(int accountNumber, String name, long balance, boolean active) {
        this.accountNumber = accountNumber;
        this.name = name;
        this.balance = balance;
        this.active = active;
    }

    /** Gets account number. */
    public int getAccountNumber() {
        return accountNumber;
    }

    /** Gets current balance. */
    public long getBalance() {
        return balance;
    }

    /** Sets current balance. */
    public void setBalance(long balance) {
        this.balance = balance;
    }

    /** Is account active. */
    public boolean isActive() {
        return active;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "Account{" +
                "accountNumber=" + accountNumber +
                ", name='" + name + '\'' +
                ", balance=" + balance +
                ", active=" + active +
                '}';
    }
}
