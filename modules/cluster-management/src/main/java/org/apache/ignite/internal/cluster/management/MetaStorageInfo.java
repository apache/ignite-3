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

package org.apache.ignite.internal.cluster.management;

import java.io.Serializable;
import java.util.Set;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/**
 * Holds information about Metastorage that is stored in the CMG.
 */
@Transferable(CmgMessageGroup.METASTORAGE_INFO)
public interface MetaStorageInfo extends NetworkMessage, Serializable {
    /**
     * Names of the nodes that host Meta storage.
     */
    Set<String> metaStorageNodes();

    /**
     * Raft index in the Metastorage group under which the forced configuration is (or will be) saved, or {@code null} if no MG
     * repair happened in the current cluster incarnation.
     *
     * <p>This can only contain index corresponding to a repair which happened in the current incarnation of the cluster (with the current
     * cluster ID); if there were earlier MG repairs, they happened in other incarnations of the cluster, so they will not leave
     * a trace here.
     */
    @Nullable Long metastorageRepairingConfigIndex();

    /**
     * Returns whether MG was repaired in this cluster incarnation.
     */
    default boolean metastorageRepairedInThisClusterIncarnation() {
        return metastorageRepairingConfigIndex() != null;
    }
}
