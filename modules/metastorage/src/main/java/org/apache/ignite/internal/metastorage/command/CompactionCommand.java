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

package org.apache.ignite.internal.metastorage.command;

import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Command to start the metastore compaction locally.
 *
 * <p>When processing the command, the following steps will be performed:</p>
 * <ul>
 *     <li>{@link KeyValueStorage#saveCompactionRevision} a new metastorage compaction revision locally.</li>
 *     <li>{@link KeyValueStorage#startCompaction} the metastorage compaction on the new revision locally.</li>
 * </ul>
 */
@Transferable(MetastorageCommandsMessageGroup.COMPACTION)
public interface CompactionCommand extends MetaStorageWriteCommand {
    /** New metastorage compaction revision. */
    long compactionRevision();
}
