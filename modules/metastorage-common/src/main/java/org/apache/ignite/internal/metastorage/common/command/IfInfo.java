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

package org.apache.ignite.internal.metastorage.common.command;

import java.io.Serializable;
import org.apache.ignite.internal.metastorage.common.StatementInfo;

/**
 * Defines if-statement for {@link MultiInvokeCommand}.
 */
public class IfInfo implements Serializable {
    /** Condition definition. */
    private final ConditionInfo cond;

    /** Definition of execution branch, if condition evaluates to true (aka left branch). */
    private final StatementInfo andThen;

    /** Definition execution branch, if condition evaluates to false (aka right branch). */
    private final StatementInfo orElse;

    /**
     * Constructs new if statement definition.
     *
     * @param cond condition.
     * @param andThen left execution branch.
     * @param orElse right execution branch.
     */
    public IfInfo(ConditionInfo cond, StatementInfo andThen,
            StatementInfo orElse) {
        this.cond = cond;
        this.andThen = andThen;
        this.orElse = orElse;
    }

    /**
     * Returns boolean condition definition.
     *
     * @return Boolean condition definition.
     */
    public ConditionInfo cond() {
        return cond;
    }

    /**
     * Returns definition of execution branch, if condition evaluates to true (aka left branch).
     *
     * @return Left execution branch definition.
     */
    public StatementInfo andThen() {
        return andThen;
    }

    /**
     * Returns definition of execution branch, if condition evaluates to false (aka right branch).
     *
     * @return Right execution branch definition.
     */
    public StatementInfo orElse() {
        return orElse;
    }
}
