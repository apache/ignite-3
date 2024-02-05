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

package org.apache.ignite.internal.index.message;

import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Request to check whether RW transactions have been finished to the requested catalog version on the node.
 *
 * <p>Check will return {@code true} under the conditions:</p>
 * <ul>
 *     <li>Requested catalog version is active on the node.</li>
 *     <li>All RW transactions started on versions strictly before the requested catalog version are finished on the node.</li>
 * </ul>
 *
 * <p>Such a check is needed, for example, to start building an index; first, we need to make sure that all RW transactions up to the
 * catalog version in which the index appeared have been finished on the active nodes of the cluster, and only then can we begin building
 * the index.</p>
 */
@Transferable(IndexMessageGroup.IS_NODE_FINISHED_RW_TRANSACTIONS_STARTED_BEFORE_REQUEST)
public interface IsNodeFinishedRwTransactionsStartedBeforeRequest extends NetworkMessage {
    /** Returns the catalog version of interest. */
    int targetCatalogVersion();
}
