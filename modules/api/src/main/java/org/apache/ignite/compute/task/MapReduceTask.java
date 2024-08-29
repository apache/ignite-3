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

package org.apache.ignite.compute.task;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.TaskDescriptor;
import org.apache.ignite.marshalling.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * A map reduce task interface. Implement this interface and pass a name of the implemented class to the
 * {@link org.apache.ignite.compute.IgniteCompute#submitMapReduce(TaskDescriptor, Object)} method to run this task.
 *
 * @param <I> Split task (I)nput type.
 * @param <M> (M)ap job input type.
 * @param <T> Map job output (T)ype and reduce job input (T)ype.
 * @param <R> Reduce (R)esult type.
 */
public interface MapReduceTask<I, M, T, R> {
    /**
     * This method should return a list of compute job execution parameters which will be used to submit compute jobs.
     *
     * @param taskContext Task execution context.
     * @param input Map reduce task (I)nput.
     * @return A future with the list of compute job execution parameters.
     */
    CompletableFuture<List<MapReduceJob<M, T>>> splitAsync(TaskExecutionContext taskContext, @Nullable I input);

    /**
     * This is a finishing step in the task execution. This method will be called with the map from identifiers of compute jobs submitted as
     * a result of the {@link #splitAsync(TaskExecutionContext, Object)} method call to the results of the execution of the corresponding
     * job. The return value of this method will be returned as a result of this task.
     *
     * @param taskContext Task execution context.
     * @param results Map from compute job ids to their results.
     * @return Final task result future.
     */
    CompletableFuture<R> reduceAsync(TaskExecutionContext taskContext, Map<UUID, T> results);

    /** The marshaller that is called to unmarshal split job argument if not null. */
    default @Nullable Marshaller<I, byte[]> splitJobInputMarshaller() {
        return null;
    }

    /** The marshaller that is called to marshal reduce job result if not null. */
    default @Nullable Marshaller<R, byte[]> reduceJobResultMarshaller() {
        return null;
    }
}
