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

package org.apache.ignite.internal.sql.engine.tx;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Context that allows to get explicit transaction provided by user or start implicit one.
 */
public interface QueryTransactionContext {
    /**
     * Starts an implicit transaction if one has not been started previously
     * and if there is no external (user-managed) transaction.
     *
     * @param readOnly Indicates whether the read-only transaction or read-write transaction should be started.
     * @param implicit Indicates whether the implicit transaction will be managed by the table storage or the SQL engine.
     * @return Transaction wrapper.
     */
    QueryTransactionWrapper getOrStartSqlManaged(boolean readOnly, boolean implicit);

    /** Updates tracker of latest time observed by client. */
    void updateObservableTime(HybridTimestamp time);

    /** Returns explicit transaction if one was provided by user. */
    @Nullable QueryTransactionWrapper explicitTx();
}
