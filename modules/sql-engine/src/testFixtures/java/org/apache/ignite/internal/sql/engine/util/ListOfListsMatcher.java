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

package org.apache.ignite.internal.sql.engine.util;

import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher for checking a list of lists.
 */
public class ListOfListsMatcher extends TypeSafeMatcher<List<List<?>>> {
    private final List<Matcher<Iterable<?>>> matchers;
    private int mismatchPosition;

    /**
     * Creates matcher.
     *
     * @param matchers Array containing matchers for the expected rows.
     */
    public ListOfListsMatcher(Matcher<Iterable<?>> ... matchers) {
        this.matchers = List.of(matchers);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("List of size ").appendValue(matchers.size())
                .appendList(" with: ", ", ", "", matchers);
    }

    @Override
    protected boolean matchesSafely(List<List<?>> item) {
        if (item.size() != matchers.size()) {
            return false;
        }

        for (int i = 0; i < matchers.size(); i++) {
            if (!matchers.get(i).matches(item.get(i))) {
                mismatchPosition = i;

                return false;
            }
        }

        return true;
    }

    @Override
    public void describeMismatchSafely(List<List<?>> actual, Description mismatchDescription) {
        if (actual.size() != matchers.size()) {
            mismatchDescription.appendText("Actual list size is ").appendValue(actual.size());

            return;
        }

        mismatchDescription.appendText("Position ")
                .appendValue(mismatchPosition)
                .appendText(" is ")
                .appendValueList("[", ", ", "]", actual.get(mismatchPosition));
    }
}
