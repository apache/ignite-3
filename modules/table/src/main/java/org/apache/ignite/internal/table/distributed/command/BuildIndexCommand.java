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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;

/** State machine command to build a table index. */
@Transferable(TableMessageGroup.Commands.BUILD_INDEX)
public interface BuildIndexCommand extends WriteCommand {
    /** Returns index ID. */
    int indexId();

    /** Returns row IDs for which to build indexes. */
    List<UUID> rowIds();

    /** Returns {@code true} if this batch is the last one. */
    boolean finish();
}
