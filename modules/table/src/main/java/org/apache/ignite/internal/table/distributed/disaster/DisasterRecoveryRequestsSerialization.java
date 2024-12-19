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

package org.apache.ignite.internal.table.distributed.disaster;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

class DisasterRecoveryRequestsSerialization {
    static void writeVarIntMap(Map<Integer, Set<Integer>> partitionIds, IgniteDataOutput out) throws IOException {
        out.writeVarInt(partitionIds.size());

        for (Map.Entry<Integer, Set<Integer>> tablePartitions : partitionIds.entrySet()) {
            out.writeVarInt(tablePartitions.getKey());

            writeVarIntSet(tablePartitions.getValue(), out);
        }
    }

    static void writeVarIntSet(Set<Integer> partitionIds, IgniteDataOutput out) throws IOException {
        out.writeVarInt(partitionIds.size());

        for (int partitionId : partitionIds) {
            out.writeVarInt(partitionId);
        }
    }

    static Map<Integer, Set<Integer>> readVarIntMap(IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        Map<Integer, Set<Integer>> map = new HashMap<>(IgniteUtils.capacity(length));

        for (int i = 0; i < length; i++) {
            int tableId = in.readVarIntAsInt();

            Set<Integer> partIds = readVarIntSet(in);

            map.put(tableId, partIds);
        }

        return map;
    }

    static Set<Integer> readVarIntSet(IgniteDataInput in) throws IOException {
        int length = in.readVarIntAsInt();

        Set<Integer> set = new HashSet<>(IgniteUtils.capacity(length));
        for (int i = 0; i < length; i++) {
            set.add(in.readVarIntAsInt());
        }

        return set;
    }
}
