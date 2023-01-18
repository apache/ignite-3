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

import org.apache.ignite.internal.metastorage.command.cursor.CursorCloseCommand;
import org.apache.ignite.internal.metastorage.command.cursor.CursorHasNextCommand;
import org.apache.ignite.internal.metastorage.command.cursor.CursorNextCommand;
import org.apache.ignite.internal.metastorage.command.cursor.CursorsCloseCommand;
import org.apache.ignite.network.annotations.MessageGroup;

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

    /** Message type for {@link GetAndPutCommand}. */
    short GET_AND_PUT = 21;

    /** Message type for {@link GetAndRemoveCommand}. */
    short GET_AND_REMOVE = 22;

    /** Message type for {@link GetAllCommand}. */
    short GET_ALL = 30;

    /** Message type for {@link GetAndPutAllCommand}. */
    short GET_AND_PUT_ALL = 31;

    /** Message type for {@link GetAndRemoveAllCommand}. */
    short GET_AND_REMOVE_ALL = 32;

    /** Message type for {@link PutCommand}. */
    short PUT = 40;

    /** Message type for {@link RemoveCommand}. */
    short REMOVE = 41;

    /** Message type for {@link PutAllCommand}. */
    short PUT_ALL = 50;

    /** Message type for {@link RemoveAllCommand}. */
    short REMOVE_ALL = 51;

    /** Message type for {@link RangeCommand}. */
    short RANGE = 60;

    /** Message type for {@link PrefixCommand}. */
    short PREFIX = 61;

    /** Message type for {@link CursorHasNextCommand}. */
    short CURSOR_HAS_NEXT = 80;

    /** Message type for {@link CursorNextCommand}. */
    short CURSOR_NEXT = 81;

    /** Message type for {@link CursorCloseCommand}. */
    short CURSOR_CLOSE = 82;

    /** Message type for {@link CursorsCloseCommand}. */
    short CURSORS_CLOSE = 83;
}
