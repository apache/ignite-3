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

package org.apache.ignite.internal.schema;

import static org.hamcrest.Matchers.is;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.hamcrest.CustomMatcher;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;

/** Matcher for comparing {@link BinaryRow}s. */
public class BinaryRowMatcher extends CustomMatcher<BinaryRow> {
    private final @Nullable BinaryRow row;

    private BinaryRowMatcher(@Nullable BinaryRow row) {
        super("Expected row to be equal to " + rowToString(row));
        this.row = row;
    }

    public static BinaryRowMatcher equalToRow(@Nullable BinaryRow row) {
        return new BinaryRowMatcher(row);
    }

    public static Matcher<BinaryRow> isRow(@Nullable BinaryRow expectedRow) {
        return is(equalToRow(expectedRow));
    }

    @Override
    public boolean matches(@Nullable Object o) {
        if (row == null || o == null) {
            return row == o; // Both are null.
        }

        if (!(o instanceof BinaryRow)) {
            return false;
        }

        BinaryRow item = (BinaryRow) o;

        return row.schemaVersion() == item.schemaVersion() && row.tupleSlice().equals(item.tupleSlice());
    }

    private static String rowToString(@Nullable BinaryRow row) {
        if (row == null) {
            return "{null row}";
        }

        ByteBuffer tupleSlice = row.tupleSlice();

        byte[] array = new byte[tupleSlice.remaining()];
        tupleSlice.get(array);

        return String.format("{schemaVersion=%d tuple=%s}", row.schemaVersion(), Arrays.toString(array));
    }
}
