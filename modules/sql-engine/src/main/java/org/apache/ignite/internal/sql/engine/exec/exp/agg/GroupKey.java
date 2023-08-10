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

package org.apache.ignite.internal.sql.engine.exec.exp.agg;

import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;

import java.util.Arrays;
import org.jetbrains.annotations.Nullable;

/**
 * A fixed length group key used by execution node.
 */
public class GroupKey {
    public static final GroupKey EMPTY_GRP_KEY = new GroupKey(OBJECT_EMPTY_ARRAY);

    private final Object[] fields;

    /** Constructor. */
    public GroupKey(Object[] fields) {
        this.fields = fields;
    }

    /** Returns {@code idx}-th} field of this key.*/
    public Object field(int idx) {
        return fields[idx];
    }

    /** The number of fields in this key. */
    public int fieldsCount() {
        return fields.length;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GroupKey groupKey = (GroupKey) o;

        return Arrays.equals(fields, groupKey.fields);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Arrays.hashCode(fields);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return "GroupKey" + Arrays.toString(fields);
    }

    public static Builder builder(int rowLen) {
        return new Builder(rowLen);
    }

    /**
     * Builder for a group key.
     */
    public static class Builder {
        private final Object[] fields;

        private int idx;

        private Builder(int rowLen) {
            fields = new Object[rowLen];
        }

        /**
         * Add a key to this group key builder.
         */
        public Builder add(@Nullable Object val) {
            if (idx == fields.length) {
                throw new IndexOutOfBoundsException();
            }

            fields[idx++] = val;

            return this;
        }

        /**
         * Builds a group key.
         */
        public GroupKey build() {
            assert idx == fields.length;

            return new GroupKey(fields);
        }
    }
}
