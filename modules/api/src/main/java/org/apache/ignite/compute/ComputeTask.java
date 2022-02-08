/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compute;

import java.util.Collection;

/**
 * Compute task interface defines a task that can be executed on the cluster. Compute task
 * handles executing individual tasks on remote nodes  and reducing
 * (aggregating) received tasks' results into final compute task result.
 *
 * @param <T> Argument type.
 * @param <I> Intermediate result's type.
 * @param <R> Reduce result's type.
 */
public interface ComputeTask<T, I, R> {
    /**
     * Executes the compute task locally on every node with a given argument.
     *
     * @param argument Compute task's argument.
     * @return Intermediate result of the computation.
     */
    I execute(T argument);

    /**
     * Reduces results of the computation from all the nodes.
     *
     * @param results Results of the computation.
     * @return Reduced result of the compute task.
     */
    R reduce(Collection<I> results);
}
