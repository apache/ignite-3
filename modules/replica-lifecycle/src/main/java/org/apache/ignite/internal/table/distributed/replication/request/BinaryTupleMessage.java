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

package org.apache.ignite.internal.table.distributed.replication.request;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.table.distributed.PartitionReplicationMessageGroup;

/**
 * Message for transferring a {@link BinaryTuple} or a {@link BinaryTuplePrefix}.
 */
@Transferable(PartitionReplicationMessageGroup.BINARY_TUPLE)
public interface BinaryTupleMessage extends NetworkMessage {
    ByteBuffer tuple();

    int elementCount();

    default BinaryTuple asBinaryTuple() {
        return new BinaryTuple(elementCount(), tuple());
    }

    default BinaryTuplePrefix asBinaryTuplePrefix() {
        return new BinaryTuplePrefix(elementCount(), tuple());
    }
}
