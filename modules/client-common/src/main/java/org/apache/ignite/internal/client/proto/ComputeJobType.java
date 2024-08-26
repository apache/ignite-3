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

package org.apache.ignite.internal.client.proto;

import org.apache.ignite.sql.ColumnType;

/**
 * The type of the object that can be passed/returned to/from the compute job. In can be a native type that is represented by
 * {@link ColumnType} or a marshalled object/tuple.
 */
public class ComputeJobType {
    static final int NATIVE_ID = 0;
    static final int MARSHALLED_TUPLE_ID = 1;
    static final int MARSHALLED_OBJECT_ID = 2;

    /**
     * [0, .., Integer.MAX_VALUE] - native types. The id is the same as in {@link ColumnType}. (0, .., Integer.MIN_VALUE] - marshalled
     * types.
     */
    private final int id;
    private final Type type;

    ComputeJobType(int id) {
        this.id = id;
        this.type = Type.fromId(id);
    }

    /** Return the id of the type. */
    public int id() {
        return id;
    }

    /** Return the type of the object. */
    public Type type() {
        return type;
    }

    /** The type of the object. */
    public enum Type {
        MARSHALLED_TUPLE, MARSHALLED_OBJECT, NATIVE;

        static Type fromId(int id) {
            if (id == MARSHALLED_TUPLE_ID) {
                return MARSHALLED_TUPLE;
            }

            if (id == MARSHALLED_OBJECT_ID) {
                return MARSHALLED_OBJECT;
            }

            if (id == NATIVE_ID) {
                return NATIVE;
            }

            throw new IllegalArgumentException("Unsupported type id: " + id);
        }
    }
}
