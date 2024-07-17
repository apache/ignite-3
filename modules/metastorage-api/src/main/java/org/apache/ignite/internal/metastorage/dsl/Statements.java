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

package org.apache.ignite.internal.metastorage.dsl;

/**
 * Static factory for various statements.
 */
public class Statements {
    private static final MetaStorageMessagesFactory MSG_FACTORY = new MetaStorageMessagesFactory();

    /**
     * Simple helper for create the new {@link Iif} statement.
     *
     * @param condition Boolean condition.
     * @param andThen Execution branch, if condition evaluates to true.
     * @param orElse Execution branch, if condition evaluates to false.
     * @return new {@link Iif} statement.
     */
    public static Iif iif(Condition condition, Iif andThen, Iif orElse) {
        return MSG_FACTORY.iif()
                .condition(condition)
                .andThen(MSG_FACTORY.ifStatement().iif(andThen).build())
                .orElse(MSG_FACTORY.ifStatement().iif(orElse).build())
                .build();
    }

    /**
     * Simple helper for create the new {@link Iif} statement.
     *
     * @param condition Boolean condition.
     * @param andThen Execution branch, if condition evaluates to true.
     * @param orElse Execution branch, if condition evaluates to false.
     * @return new {@link Iif} statement.
     */
    public static Iif iif(Condition condition, Iif andThen, Update orElse) {
        return MSG_FACTORY.iif()
                .condition(condition)
                .andThen(MSG_FACTORY.ifStatement().iif(andThen).build())
                .orElse(MSG_FACTORY.updateStatement().update(orElse).build())
                .build();
    }

    /**
     * Simple helper for create the new {@link Iif} statement.
     *
     * @param condition Boolean condition.
     * @param andThen Execution branch, if condition evaluates to true.
     * @param orElse Execution branch, if condition evaluates to false.
     * @return new {@link Iif} statement.
     */
    public static Iif iif(Condition condition, Update andThen, Iif orElse) {
        return MSG_FACTORY.iif()
                .condition(condition)
                .andThen(MSG_FACTORY.updateStatement().update(andThen).build())
                .orElse(MSG_FACTORY.ifStatement().iif(orElse).build())
                .build();
    }

    /**
     * Simple helper for create the new {@link Iif} statement.
     *
     * @param condition Boolean condition.
     * @param andThen Execution branch, if condition evaluates to true.
     * @param orElse Execution branch, if condition evaluates to false.
     * @return new {@link Iif} statement.
     */
    public static Iif iif(Condition condition, Update andThen, Update orElse) {
        return MSG_FACTORY.iif()
                .condition(condition)
                .andThen(MSG_FACTORY.updateStatement().update(andThen).build())
                .orElse(MSG_FACTORY.updateStatement().update(orElse).build())
                .build();
    }

    private Statements() {
    }
}
