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

package org.apache.ignite.internal.sql.engine.metadata;

import java.util.UUID;
import java.util.function.ToIntFunction;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for creating a function to calculate the hash of the specified fields of the row.
 */
public interface HashFunctionFactory<T> {
    /**
     * Creates a hash function on the basis of which destination nodes are calculated.
     *
     * @param typesAware {@code True} to create a hash function that will take into account the types of the row's fields.
     * @param fields Field ordinals of the row from which the hash is to be calculated.
     * @param tableId Table ID.
     * @return Function to compute a composite hash of the specified fields of the row.
     */
    ToIntFunction<T> create(boolean typesAware, int[] fields, @Nullable UUID tableId);
}
