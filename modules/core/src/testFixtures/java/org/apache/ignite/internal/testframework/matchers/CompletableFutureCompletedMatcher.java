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

import java.util.concurrent.CompletableFuture;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/** A matcher that tests if a CompletableFuture has completed successfully. */
public class CompletableFutureCompletedMatcher<T> extends TypeSafeMatcher<CompletableFuture<T>> {
    private CompletableFutureCompletedMatcher() {
    }

    @Override
    protected boolean matchesSafely(CompletableFuture<T> future) {
        return future.isDone() && !future.isCompletedExceptionally() && !future.isCancelled();
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("is a successfully completed CompletableFuture");
    }

    @Override
    public void describeMismatchSafely(CompletableFuture<T> item, Description mismatchDescription) {
        if (!item.isDone()) {
            mismatchDescription.appendText("was not completed");
        } else {
            if (item.isCompletedExceptionally()) {
                mismatchDescription.appendText("completed exceptionally");
            } else if (item.isCancelled()) {
                mismatchDescription.appendText("was cancelled");
            } else {
                // It might be successfully done now, but it wasn't at the moment of matchesSafely execution.
                mismatchDescription.appendText("was not completed");
            }
        }
    }

    /**
     * Creates a {@link CompletableFutureCompletedMatcher}.
     */
    public static <T> CompletableFutureCompletedMatcher<T> completedFuture() {
        return new CompletableFutureCompletedMatcher<T>();
    }
}
