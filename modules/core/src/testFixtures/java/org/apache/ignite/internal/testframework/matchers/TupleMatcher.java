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

package org.apache.ignite.internal.testframework.matchers;

import org.apache.ignite.table.Tuple;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;

/**
 * Matchers for {@link Tuple}.
 */
public final class TupleMatcher {
    private TupleMatcher() {
    }

    /**
     * Creates a matcher for matching column value.
     *
     * @param columnName Column name in SQL-parser style notation; e.g., <br>
     *                   "myColumn" - "MYCOLUMN", returns the index of the column ignores case sensitivity, <br>
     *                   "\"MyColumn\"" - "MyColumn", returns the index of the column with respect to case sensitivity.
     * @param valueMatcher Matcher for matching column value.
     * @return Matcher for matching column value with given value.
     */
    public static <T> Matcher<Tuple> tupleValue(String columnName, Matcher<T> valueMatcher) {
        return new FeatureMatcher<>(valueMatcher, "A tuple with value", "value") {
            @Override
            protected @Nullable T featureValueOf(Tuple actual) {
                return actual.value(columnName);
            }
        };
    }
}
