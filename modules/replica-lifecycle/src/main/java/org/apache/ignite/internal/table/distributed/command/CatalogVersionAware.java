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

package org.apache.ignite.internal.table.distributed.command;

import org.apache.ignite.internal.datareplication.marshaller.PartitionCommandsMarshaller;

/**
 * A command that requires certain level of catalog version to be locally available just to be accepted on the node.
 */
public interface CatalogVersionAware {
    /**
     * Returns version that the Catalog must have locally for the node to be allowed to accept this command via replication.
     */
    int requiredCatalogVersion();

    /**
     * Setter for {@link #requiredCatalogVersion()}. Called by the creator or the {@link PartitionCommandsMarshaller} while deserializing.
     */
    void requiredCatalogVersion(int version);
}
