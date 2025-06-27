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

package org.apache.ignite.internal.partition.replicator.network.command;

import java.util.Set;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;

/**
 * State machine command to cleanup on a transaction commit.
 *
 * <p>Unline {@link WriteIntentSwitchCommand}, this implementation can be used in both collocated and non-collocated zones, and should be
 * considered its replacement. Old command is kept for backward compatibility and should not be instantiated outside of tests.
 */
@Transferable(PartitionReplicationMessageGroup.Commands.WRITE_INTENT_SWITCH_V2)
public interface WriteIntentSwitchCommandV2 extends WriteIntentSwitchCommand {
    /** IDs of tables in which the partition in question had write intents. */
    Set<Integer> tableIds();
}
