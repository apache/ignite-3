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

package org.apache.ignite.internal.tx.test;

import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.equalTo;

import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.tx.LockMode;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class LockFutureMatcher extends TypeSafeMatcher<CompletableFuture<Lock>> {
    private final Matcher<Lock> lockModeMatcher;

    public LockFutureMatcher(Matcher<Lock> lockModeMatcher) {
        this.lockModeMatcher = lockModeMatcher;
    }

    @Override
    protected boolean matchesSafely(CompletableFuture<Lock> item) {
        try {
            Lock lock = item.get(100, TimeUnit.MILLISECONDS);

            return lockModeMatcher.matches(lock);
        } catch (ExecutionException | CancellationException | InterruptedException | TimeoutException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    protected void describeMismatchSafely(CompletableFuture<Lock> item, Description mismatchDescription) {
        if (item.isDone()) {
            lockModeMatcher.describeMismatch(item.join(), mismatchDescription);
        } else {
            mismatchDescription.appendText("was ").appendValue(item);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("is a lock future that completes successfully with ").appendDescriptionOf(lockModeMatcher);
    }

    public static LockFutureMatcher isGranted(LockKey key, LockMode lockMode, UUID owner) {
        return new LockFutureMatcher(equalTo(new Lock(key, lockMode, owner)));
    }
}
