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

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/** Matcher for comparing {@link BinaryRow}s. */
public class BinaryRowMatcher extends TypeSafeMatcher<BinaryRow> {
    private final BinaryRow row;

    private BinaryRowMatcher(BinaryRow row) {
        this.row = row;
    }

    public static BinaryRowMatcher equalToRow(BinaryRow row) {
        return new BinaryRowMatcher(row);
    }

    @Override
    protected boolean matchesSafely(BinaryRow item) {
        return row.schemaVersion() == item.schemaVersion() && row.tupleSlice().equals(item.tupleSlice());
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue(rowToString(row));
    }

    @Override
    protected void describeMismatchSafely(BinaryRow item, Description mismatchDescription) {
        mismatchDescription.appendText("was ").appendValue(rowToString(item));
    }

    private static String rowToString(BinaryRow row) {
        ByteBuffer tupleSlice = row.tupleSlice();

        byte[] array = new byte[tupleSlice.remaining()];
        tupleSlice.get(array);

        return String.format("{schemaVersion=%d tuple=%s}", row.schemaVersion(), Arrays.toString(array));
    }
}
