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

package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.wrapper.Wrappers.unwrap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;

/** A job that flushes all partition storages of a table on the node. */
public class FlushTablePartitionsJob implements ComputeJob<String, Void> {
    @Override
    public CompletableFuture<Void> executeAsync(JobExecutionContext context, String tableName) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        TableViewInternal table = unwrap(context.ignite().tables().table(tableName), TableViewInternal.class);
        InternalTable internalTable = table.internalTable();

        for (int partitionId = 0; partitionId < internalTable.partitions(); partitionId++) {
            MvPartitionStorage partitionStorage = internalTable.storage().getMvPartition(partitionId);
            if (partitionStorage != null) {
                futures.add(partitionStorage.flush());
            }
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }
}
