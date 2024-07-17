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

package org.apache.ignite.internal.metastorage.server;

import org.apache.ignite.internal.metastorage.dsl.Update;

/**
 * Root building block for the compound meta storage invoke command.
 * Contains of boolean condition and 2 branches of execution, like usual programming language's if.
 * Every branch can be either a new {@link If} statement (non-terminal) or a result terminal statement {@link Update}.
 */
public class If {
    /** Boolean condition. */
    private final Condition cond;

    /** Execution branch, if condition evaluates to true (aka left branch). */
    private final Statement andThen;

    /** Execution branch, if condition evaluates to false (aka right branch). */
    private final Statement orElse;

    /**
     * Construct new {@link If} statement.
     *
     * @param cond Boolean condition.
     * @param andThen Left execution branch.
     * @param orElse Right execution branch.
     */
    public If(Condition cond, Statement andThen, Statement orElse) {
        this.cond = cond;
        this.andThen = andThen;
        this.orElse = orElse;
    }

    /**
     * Returns boolean condition.
     *
     * @return Boolean condition.
     */
    public Condition cond() {
        return cond;
    }

    /**
     * Execution branch, if condition evaluates to true (aka left branch).
     *
     * @return Left execution branch.
     */
    public Statement andThen() {
        return andThen;
    }

    /**
     * Execution branch, if condition evaluates to false (aka right branch).
     *
     * @return Right execution branch.
     */
    public Statement orElse() {
        return orElse;
    }
}
