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

package org.apache.ignite.internal.metastorage.command.info;

import java.io.Serializable;
import org.apache.ignite.internal.metastorage.command.MetastorageCommandsMessageGroup;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Definition of simple Either-like wrapper to hold one of the statement type: {@link IfInfo} or {@link UpdateInfo}.
 * Needed to construct and simple deconstruction of nested {@link IfInfo},
 * instead of empty interface and instanceof-based unwrap.
 *
 * @see IfInfo
 * @see UpdateInfo
 */
@Transferable(MetastorageCommandsMessageGroup.STATEMENT_INFO)
public interface StatementInfo extends NetworkMessage, Serializable {
    /**
     * Returns {@link IfInfo} or {@code null}, if iif is not defined.
     * Note: check which field is filled by {@link #isTerminal()}
     *
     * @return {@link IfInfo} or {@code null}, if iif is not defined.
     */
    IfInfo iif();

    /**
     * Returns {@link UpdateInfo} or {@code null}, if update is not defined.
     * Note: check which field is filled by {@link #isTerminal()}
     *
     * @return {@link UpdateInfo} or {@code null}, if update is not defined.
     */
    UpdateInfo update();

    /**
     * Returns true, if statement has no nested statement (i.e. it is {@link UpdateInfo} statement definition).
     *
     * @return true, if statement has no nested statement (i.e. it is {@link UpdateInfo} statement definition).
     */
    default boolean isTerminal() {
        return update() != null;
    }
}
