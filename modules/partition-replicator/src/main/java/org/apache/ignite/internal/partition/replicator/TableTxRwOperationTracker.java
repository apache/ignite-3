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

package org.apache.ignite.internal.partition.replicator;

import java.util.concurrent.CompletableFuture;

/**
 * Tracks the completion of RW transactions' operations before a dependent process begins.
 */
public interface TableTxRwOperationTracker {
    /**
     * Waits for RW transactions operations to complete strictly lower than the requested catalog version.
     *
     * @param catalogVersion Catalog version in question.
     */
    CompletableFuture<Void> awaitCompleteTxRwOperations(int catalogVersion);
}
