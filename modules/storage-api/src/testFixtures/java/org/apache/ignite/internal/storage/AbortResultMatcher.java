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

import java.util.Objects;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

/** Matcher for comparing {@link AbortResult}s. */
public class AbortResultMatcher extends TypeSafeMatcher<AbortResult> {
    private final AbortResult expected;

    private final Matcher<BinaryRow> binaryRowMatcher;

    private AbortResultMatcher(AbortResult expected) {
        this.expected = expected;

        BinaryRow row = expected.previousWriteIntent();
        this.binaryRowMatcher = row == null ? Matchers.nullValue(BinaryRow.class) : BinaryRowMatcher.equalToRow(row);
    }

    @Override
    protected boolean matchesSafely(AbortResult actual) {
        return expected.status() == actual.status()
                && Objects.equals(expected.expectedTxId(), actual.expectedTxId())
                && binaryRowMatcher.matches(actual.previousWriteIntent());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("was ").appendValue(expected);
    }

    public static AbortResultMatcher equalsToAbortResult(AbortResult expected) {
        return new AbortResultMatcher(expected);
    }
}
