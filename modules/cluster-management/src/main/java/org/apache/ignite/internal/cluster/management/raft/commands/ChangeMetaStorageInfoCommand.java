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

package org.apache.ignite.internal.cluster.management.raft.commands;

import java.util.Set;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.raft.WriteCommand;
import org.jetbrains.annotations.Nullable;

/**
 * Command for changing metastorage nodes.
 */
@Transferable(CmgMessageGroup.Commands.CHANGE_METASTORAGE_INFO)
public interface ChangeMetaStorageInfoCommand extends WriteCommand {
    /**
     * Names of the nodes that host Meta storage.
     */
    Set<String> metaStorageNodes();

    /**
     * Raft index in the Metastorage group under which the forced configuration is (or will be) saved, or {@code null} if no MG
     * repair happened in the current cluster incarnation.
     */
    @Nullable Long metastorageRepairingConfigIndex();
}
