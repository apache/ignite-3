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

package org.apache.ignite.internal.metastorage.common;

import java.io.Serializable;
import org.apache.ignite.internal.metastorage.common.command.IfInfo;

/**
 * Definition of simple Either-like wrapper to hold one of the statement type: {@link IfInfo} or {@link UpdateInfo}.
 * Needed to construct and simple deconstruction of nested {@link IfInfo},
 * instead of empty interface and instanceof-based unwrap.
 *
 * @see IfInfo
 * @see UpdateInfo
 */
public class StatementInfo implements Serializable {
    /** If definition holder. */
    private final IfInfo iif;

    /** Update definition holder. */
    private final UpdateInfo update;

    /**
     * Constructs new {@link IfInfo} statement definition.
     *
     * @param iif If statement definition
     */
    public StatementInfo(IfInfo iif) {
        this.iif = iif;
        this.update = null;
    }

    /**
     * Constructs new {@link UpdateInfo} terminal statement definition.
     *
     * @param update Update statement definition
     */
    public StatementInfo(UpdateInfo update) {
        this.update = update;
        this.iif = null;
    }

    /**
     * Returns true, if statement has no nested statement (i.e. it is {@link UpdateInfo} statement definition).
     *
     * @return true, if statement has no nested statement (i.e. it is {@link UpdateInfo} statement definition).
     */
    public boolean isTerminal() {
        return update != null;
    }

    /**
     * Returns {@link IfInfo} or {@code null}, if iif is not defined.
     * Note: check which field is filled by {@link #isTerminal()}
     *
     * @return {@link IfInfo} or {@code null}, if iif is not defined.
     */
    public IfInfo iif() {
        return iif;
    }

    /**
     * Returns {@link UpdateInfo} or {@code null}, if update is not defined.
     * Note: check which field is filled by {@link #isTerminal()}
     *
     * @return {@link UpdateInfo} or {@code null}, if update is not defined.
     */
    public UpdateInfo update() {
        return update;
    }
}
