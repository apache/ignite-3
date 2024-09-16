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

package org.apache.ignite.internal.partition.replicator.network.replication;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.replicator.message.SchemaVersionAwareReplicaRequest;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowImpl;

/**
 * Multiple row replica request.
 */
public interface MultipleRowReplicaRequest extends SchemaVersionAwareReplicaRequest {
    List<ByteBuffer> binaryTuples();

    /**
     * Returns {@link BinaryRow}s contained in the request.
     */
    default List<BinaryRow> binaryRows() {
        List<ByteBuffer> tuples = binaryTuples();
        List<BinaryRow> rows = new ArrayList<>(tuples.size());

        for (ByteBuffer tuple : tuples) {
            rows.add(new BinaryRowImpl(schemaVersion(), tuple));
        }

        return rows;
    }

    /** Transaction operation type. */
    RequestType requestType();
}
