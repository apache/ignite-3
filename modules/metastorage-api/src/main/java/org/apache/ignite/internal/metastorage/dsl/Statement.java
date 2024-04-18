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

import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Simple Either-like wrapper to hold one of the statement type: {@link Iif} or {@link Update}.
 * Needed to construct and simple deconstruction of nested {@link Iif},
 * instead of empty interface and instanceof-based unwrap.
 */
public interface Statement extends NetworkMessage {

    /**
     * Statement containing an {@code If} type.
     */
    @Transferable(MetaStorageMessageGroup.IF_STATEMENT)
    interface IfStatement extends Statement {
        /** If value holder. */
        Iif iif();
    }

    /**
     * Statement containing an {@code Update} type.
     */
    @Transferable(MetaStorageMessageGroup.UPDATE_STATEMENT)
    interface UpdateStatement extends Statement {
        /** Update value holder. */
        Update update();
    }
}
