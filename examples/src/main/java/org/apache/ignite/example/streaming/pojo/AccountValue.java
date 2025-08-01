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
 * POJO class that represents Account value.
 */
public class AccountValue {
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
    public AccountValue() {
    }

    /**
     * Constructor.
     *
     * @param name Name.
     * @param balance Balance.
     * @param active Is account active.
     */
    public AccountValue(String name, long balance, boolean active) {
        this.name = name;
        this.balance = balance;
        this.active = active;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "AccountValue{" +
                "name='" + name + '\'' +
                ", balance=" + balance +
                ", active=" + active +
                '}';
    }
}
