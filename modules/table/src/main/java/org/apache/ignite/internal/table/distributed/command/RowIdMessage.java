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

import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

import java.io.Serializable;
import java.util.UUID;

/**
 *
 */
@Transferable(TableMessageGroup.Commands.ROW_ID)
public interface RowIdMessage extends NetworkMessage, Serializable {
    short partitionId();

    UUID uuid();

    default RowId asRowId() {
        return new RowId(partitionId(), uuid().getMostSignificantBits(), uuid().getLeastSignificantBits());
    }

    static RowIdMessage fromRowId(TableMessagesFactory msgFactory, RowId rowId) {
        return (RowIdMessage) msgFactory.rowIdMessage()
                .uuid(new UUID(rowId.mostSignificantBits(), rowId.leastSignificantBits()))
                .partitionId((short) rowId.partitionId())
                .build();
    }
}
