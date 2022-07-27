/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.replicator.message;

import java.util.UUID;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Cleanup request. The message is sent for removing an open resources on the replica side.
 */
@Transferable(ReplicaMessageGroup.CLEANUP_REQUEST)
public interface CleanupRequest extends ReplicaRequest {

    /**
     * Gets a context id that identifies an operation group to clean.
     *
     * @return Operation context.
     */
    UUID operationContextId();
}
