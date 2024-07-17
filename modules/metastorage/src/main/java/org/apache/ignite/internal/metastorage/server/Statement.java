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
 * Simple Either-like wrapper to hold one of the statement type: {@link If} or {@link Update}.
 * Needed to construct and simple deconstruction of nested {@link If},
 * instead of empty interface and instanceof-based unwrap.
 *
 * @see If
 * @see Update
 */
public class Statement {
    /** If value holder. */
    private final If iif;

    /** Update value holder. */
    private final Update update;

    /**
     * Constructs new {@link If} statement.
     *
     * @param iif If
     */
    public Statement(If iif) {
        this.iif = iif;
        this.update = null;
    }

    /**
     * Constructs new {@link Update} terminal statement.
     *
     * @param update Update
     */
    public Statement(Update update) {
        this.update = update;
        this.iif = null;
    }

    /**
     * Returns true, if statement has no nested statement (i.e. it is {@link Update} statement).
     *
     * @return true, if statement has no nested statement (i.e. it is {@link Update} statement).
     */
    public boolean isTerminal() {
        return update != null;
    }

    /**
     * Returns If or {@code null}, if no If value.
     * Note: check which field is filled by {@link #isTerminal()}
     *
     * @return If or {@code null}, if no If value.
     */
    public If iif() {
        return iif;
    }


    /**
     * Returns Update or {@code null}, if no update value.
     * Note: check which field is filled by {@link #isTerminal()}
     *
     * @return Update or {@code null}, if no update value.
     */
    public Update update() {
        return update;
    }
}
