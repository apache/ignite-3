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
import org.apache.ignite.internal.tx.PossibleDeadlockOnLockAcquireException;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class LockConflictMatcher extends TypeSafeMatcher<CompletableFuture<Lock>> {
    private final UUID conflictId;
    private CompletableFuture<Lock> item;

    private LockConflictMatcher(UUID txId) {
        this.conflictId = txId;
    }

    @Override
    protected boolean matchesSafely(CompletableFuture<Lock> item) {
        try {
            this.item = item;
            item.get(100, TimeUnit.MILLISECONDS);
            return false; // Exception is expected.
        } catch (InterruptedException | TimeoutException e) {
            throw new AssertionError(e);
        } catch (ExecutionException | CancellationException e) {
            Throwable cause = ExceptionUtils.unwrapCause(e);

            if (cause instanceof PossibleDeadlockOnLockAcquireException && conflictId != null) {
                PossibleDeadlockOnLockAcquireException e0 = (PossibleDeadlockOnLockAcquireException) cause;

                return e0.getMessage().contains(conflictId.toString());
            }

            throw new AssertionError(e);
        }
    }

    @Override
    protected void describeMismatchSafely(CompletableFuture<Lock> item, Description mismatchDescription) {
        if (item.isDone() && !item.isCompletedExceptionally()) {
            mismatchDescription.appendText("lock future completes without a conflict on a locker ").appendValue(conflictId);
        } else {
            mismatchDescription.appendText("was ").appendValue(item);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("lock future which awaits conflict on ").appendValue(conflictId);
    }

    public static LockConflictMatcher conflictsWith(UUID txId) {
        return new LockConflictMatcher(txId);
    }
}
