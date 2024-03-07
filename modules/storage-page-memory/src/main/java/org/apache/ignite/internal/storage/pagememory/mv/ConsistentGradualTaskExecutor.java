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

package org.apache.ignite.internal.storage.pagememory.mv;

import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.pagememory.util.GradualTask;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.StorageException;

/**
 * A {@link GradualTaskExecutor} that wraps every task step into {@link MvPartitionStorage#runConsistently}.
 */
class ConsistentGradualTaskExecutor extends GradualTaskExecutor {
    private final MvPartitionStorage mvPartitionStorage;

    ConsistentGradualTaskExecutor(MvPartitionStorage partitionStorage, ExecutorService executor) {
        super(executor);

        this.mvPartitionStorage = partitionStorage;
    }

    @Override
    protected void runStep(GradualTask task) {
        mvPartitionStorage.runConsistently(locker -> {
            try {
                task.runStep();

                return null;
            } catch (StorageException e) {
                throw e;
            } catch (Exception e) {
                throw new StorageException(e);
            }
        });
    }
}
