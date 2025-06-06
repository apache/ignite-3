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
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/** Matcher for comparing {@link AddWriteCommittedResult}s. */
public class AddWriteCommittedResultMatcher extends TypeSafeMatcher<AddWriteCommittedResult> {
    private final AddWriteCommittedResult expected;

    private AddWriteCommittedResultMatcher(AddWriteCommittedResult expected) {
        this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(AddWriteCommittedResult actual) {
        return expected.status() == actual.status()
                && Objects.equals(expected.currentWriteIntentTxId(), actual.currentWriteIntentTxId())
                && Objects.equals(expected.latestCommitTimestamp(), actual.latestCommitTimestamp());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("was ").appendValue(expected);
    }

    public static AddWriteCommittedResultMatcher equalsToAddWriteCommittedResult(AddWriteCommittedResult expected) {
        return new AddWriteCommittedResultMatcher(expected);
    }
}
