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

package org.apache.ignite.internal.compute.utils;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class for describing mismatches in Hamcrest matchers containing multiple matchers.
 */
public class MismatchesDescriptor {
    private boolean hasMismatches;

    private final Description mismatchDescription;

    public MismatchesDescriptor(Description mismatchDescription) {
        this.mismatchDescription = mismatchDescription;
    }

    /**
     * Append mismatch description if {@code matcher} is not {@code null} and {@code actual} is not matched.
     *
     * @param matcher Matcher.
     * @param actual Actual value.
     * @param description Description of the field.
     * @param <T> Type of the value.
     * @return This instance for chaining.
     */
    public <T> MismatchesDescriptor describeMismatch(@Nullable Matcher<T> matcher, T actual, String description) {
        if (matcher != null && !matcher.matches(actual)) {
            if (hasMismatches) {
                mismatchDescription.appendText(" and ");
            }
            mismatchDescription.appendText(description).appendText(" ");
            matcher.describeMismatch(actual, mismatchDescription);
            hasMismatches = true;
        }
        return this;
    }

}
