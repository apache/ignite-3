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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.List;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;

/**
 * Returns a list of partitions computed at mapping stage. This provider never returns an empty list.
 */
public class StaticPartitionProvider<RowT> implements PartitionProvider<RowT> {

    private final List<PartitionWithConsistencyToken> partitions;

    /** Constructor. */
    public StaticPartitionProvider(String nodeName, ColocationGroup colocationGroup) {
        List<PartitionWithConsistencyToken> partitions = colocationGroup.partitionsWithConsistencyTokens(nodeName);
        assert !partitions.isEmpty() : format("No partitions for node {} group: {}", nodeName, colocationGroup);

        this.partitions = partitions;
    }

    /** {@inheritDoc} */
    @Override
    public List<PartitionWithConsistencyToken> getPartitions(ExecutionContext<RowT> ctx) {
        return partitions;
    }
}
