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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Interface that provides compute functionality allowing to execute {@link ComputeTask}s.
 */
public interface IgniteCompute {
    /**
     * Executes compute task.
     *
     * @param clazz Task class.
     * @param argument Argument object.
     * @param <T> Argument type.
     * @param <I> Intermediate result's type.
     * @param <R> Return type.
     * @return Computation result.
     */
    <T, I, R> R execute(@NotNull Class<? extends ComputeTask<T, I, R>> clazz, @Nullable T argument) throws ComputeException;
}
