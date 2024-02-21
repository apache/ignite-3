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

package org.apache.ignite.internal.storage;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

/** Matcher for comparing {@link BinaryRowAndRowId}s. */
public class BinaryRowAndRowIdMatcher extends TypeSafeMatcher<BinaryRowAndRowId> {
    private final RowId rowId;

    private final Matcher<BinaryRow> binaryRowMatcher;

    private BinaryRowAndRowIdMatcher(BinaryRowAndRowId expected) {
        this.rowId = expected.rowId();

        BinaryRow row = expected.binaryRow();
        this.binaryRowMatcher = row == null ? Matchers.nullValue(BinaryRow.class) : BinaryRowMatcher.equalToRow(row);
    }

    public static BinaryRowAndRowIdMatcher equalToBinaryRowAndRowId(BinaryRowAndRowId exp) {
        return new BinaryRowAndRowIdMatcher(exp);
    }

    @Override
    protected boolean matchesSafely(BinaryRowAndRowId item) {
        return rowId.equals(item.rowId()) && binaryRowMatcher.matches(item.binaryRow());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("was rowId").appendValue(rowId);
        description.appendText(" binaryRow");
        binaryRowMatcher.describeTo(description);
    }
}
