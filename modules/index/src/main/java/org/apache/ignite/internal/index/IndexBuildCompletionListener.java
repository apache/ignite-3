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

package org.apache.ignite.internal.index;

import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;

/** Index build completion listener, will be called when a distributed build of an index for a specific partition completes. */
interface IndexBuildCompletionListener {
    /** Handles the index build completion event. */
    default void onBuildCompletion(int indexId, int tableId, int partitionId) {
        // No-op.
    }

    /** Handles an {@link CatalogIndexStatus#AVAILABLE available} index build completion event after disaster recovery. */
    default void onBuildCompletionAfterDisasterRecovery(int indexId, int tableId, int partitionId) {
        // No-op.
    }
}
