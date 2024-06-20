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

package org.apache.ignite.compute;

import java.util.Objects;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/**
 * Colocated job execution target.
 */
public class ColocatedJobTarget implements JobTarget {
    private final String tableName;

    private final Object key;

    private final @Nullable Mapper<?> keyMapper;

    ColocatedJobTarget(String tableName, Object key, @Nullable Mapper<?> keyMapper) {
        Objects.requireNonNull(tableName);
        Objects.requireNonNull(key);

        if (keyMapper == null && !(key instanceof Tuple)) {
            throw new IllegalArgumentException("Key must be an instance of Tuple when keyMapper is not provided.");
        }

        this.tableName = tableName;
        this.key = key;
        this.keyMapper = keyMapper;
    }

    public String tableName() {
        return tableName;
    }

    public Object key() {
        return key;
    }

    public @Nullable Mapper<?> keyMapper() {
        return keyMapper;
    }
}
