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

package org.apache.ignite.internal.catalog.compaction.message;

import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/**
 * Result of tx time propagation to replicas.
 *
 * @see CatalogCompactionPrepareUpdateTxBeginTimeRequest
 */
@Transferable(CatalogCompactionMessageGroup.PREPARE_TO_UPDATE_TIME_ON_REPLICAS_RESPONSE)
public interface CatalogCompactionPrepareUpdateTxBeginTimeResponse extends NetworkMessage {
    /** Returns {@code true} if propagation was successful, {@code false} if propagation was aborted without errors. */
    boolean success();

    /** Returns an error message if propagation was aborted due to an error. */
    @Nullable String error();
}
