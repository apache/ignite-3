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

package org.apache.ignite.internal.sql.engine.message;

import org.apache.ignite.internal.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/**
 * A message to notify remote fragment (aka remote source) that more batches required to fulfil the result.
 */
@Transferable(SqlQueryMessageGroup.QUERY_BATCH_REQUEST)
public interface QueryBatchRequestMessage extends ExecutionContextAwareMessage {
    /** Returns an identifier of the exchange to request batches from. */
    long exchangeId();

    /** Returns amount of batches to request. */
    int amountOfBatches();

    /** Returns a state that should be propagated to the target fragment. */
    @Nullable SharedStateMessage sharedStateMessage();
}
