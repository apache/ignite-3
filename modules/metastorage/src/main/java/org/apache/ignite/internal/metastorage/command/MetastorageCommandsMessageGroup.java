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

import org.apache.ignite.internal.network.annotations.MessageGroup;

/**
 * Message group for meta-storage RAFT commands and other required classes.
 */
@MessageGroup(groupType = 111, groupName = "MetaStorageCommands")
public interface MetastorageCommandsMessageGroup {
    /** Message type for {@link InvokeCommand}. */
    short INVOKE = 10;

    /** Message type for {@link MultiInvokeCommand}. */
    short MULTI_INVOKE = 11;

    //----------------------------------

    /** Message type for {@link GetCommand}. */
    short GET = 20;

    /** Message type for {@link GetAllCommand}. */
    short GET_ALL = 30;

    /** Message type for {@link GetCurrentRevisionCommand}. */
    short GET_CURRENT_REVISION = 33;

    /** Message type for {@link PutCommand}. */
    short PUT = 40;

    /** Message type for {@link RemoveCommand}. */
    short REMOVE = 41;

    /** Message type for {@link PutAllCommand}. */
    short PUT_ALL = 50;

    /** Message type for {@link RemoveAllCommand}. */
    short REMOVE_ALL = 51;

    /** Message type for {@link GetRangeCommand}. */
    short GET_RANGE = 60;

    /** Message type for {@link GetPrefixCommand}. */
    short GET_PREFIX = 61;

    /** Message type for {@link SyncTimeCommand}. */
    short SYNC_TIME = 70;

    /** Message type for {@link EvictIdempotentCommandsCacheCommand}. */
    short EVICT_IDEMPOTENT_COMMAND_CACHE = 71;
}
