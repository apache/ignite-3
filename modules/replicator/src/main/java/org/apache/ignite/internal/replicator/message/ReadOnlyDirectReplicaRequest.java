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

package org.apache.ignite.internal.replicator.message;

/**
 * Read-only request that sand to a specific node directly. This request has no read timestamp as other read-only requests because the
 * timestamp is calculated on the replica side. The linearization is guaranteed by sending the request directly to the primary node.
 *
 * <p>The requests are used to implement an implicit read-only transaction for a single partition.
 */
public interface ReadOnlyDirectReplicaRequest extends PrimaryReplicaRequest {
}
