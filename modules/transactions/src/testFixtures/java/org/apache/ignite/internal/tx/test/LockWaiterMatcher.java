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

import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.tx.Lock;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class LockWaiterMatcher extends TypeSafeMatcher<CompletableFuture<Lock>> {
    private final UUID waiterId;
    private CompletableFuture<Lock> item;

    private LockWaiterMatcher(UUID txId) {
        this.waiterId = txId;
    }

    @Override
    protected boolean matchesSafely(CompletableFuture<Lock> item) {
        try {
            this.item = item;
            item.get(100, TimeUnit.MILLISECONDS);
            return false; // Timeout exception is expected.
        } catch (TimeoutException e) {
            return true;
        } catch (InterruptedException | ExecutionException | CancellationException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    protected void describeMismatchSafely(CompletableFuture<Lock> item, Description mismatchDescription) {
        mismatchDescription.appendText("lock future is completed ").appendValue(item);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("lock future which should wait for ").appendValue(waiterId);
    }

    public static LockWaiterMatcher waitsFor(UUID... txIds) {
        return new LockWaiterMatcher(txIds[0]);
    }
}
