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

import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * The type of the object that can be passed/returned to/from the compute job. In can be a native type that is represented by
 * {@link ColumnType} or a marshalled object/tuple.
 */
public enum ComputeJobDataType {
    NATIVE(0),
    TUPLE(1),
    MARSHALLED_CUSTOM(2),
    POJO(3),
    TUPLE_COLLECTION(4);

    private static final ComputeJobDataType[] VALUES = values();

    private final int id;

    ComputeJobDataType(int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    /**
     * Returns enum value corresponding to the id.
     *
     * @param id Identifier of the value.
     * @return Enum value or {@code null} if identifier is invalid.
     */
    public static @Nullable ComputeJobDataType fromId(int id) {
        return id >= 0 && id < VALUES.length ? VALUES[id] : null;
    }
}
