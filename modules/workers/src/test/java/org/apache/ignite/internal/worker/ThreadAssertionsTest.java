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

package org.apache.ignite.internal.worker;

import static org.apache.ignite.internal.thread.PublicApiThreading.execUserSyncOperation;
import static org.apache.ignite.internal.thread.ThreadOperation.NOTHING_ALLOWED;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SystemPropertiesExtension.class)
@WithSystemProperty(key = IgniteSystemProperties.THREAD_ASSERTIONS_LOG_BEFORE_THROWING, value = "false")
class ThreadAssertionsTest {

    private final Runnable assertAllowedToWrite = () -> ThreadAssertions.assertThreadAllowsTo(STORAGE_WRITE);

    @Test
    void noAssertionWhenThreadHasPermission() {
        assertDoesNotThrow(() -> executeWithPermissions(assertAllowedToWrite, STORAGE_WRITE));
    }

    @Test
    void noAssertionWhenNonIgniteThreadIsMarkedAsExecutingUserSyncOperation() {
        execUserSyncOperation(() -> assertDoesNotThrow(assertAllowedToWrite::run));
    }

    @Test
    void throwsAssertionWhenThreadHasNoPermission() {
        AssertionError err = assertThrows(
                AssertionError.class,
                () -> executeWithPermissions(assertAllowedToWrite, NOTHING_ALLOWED)
        );

        assertThat(err.getMessage(), is("Thread %test%test is not allowed to do STORAGE_WRITE"));
    }

    @Test
    void throwsAssertionWhenThreadHasNoPermissionButMarkedAsExecutingUserSyncOpereation() {
        AssertionError err = assertThrows(
                AssertionError.class,
                () -> executeWithPermissions(() -> execUserSyncOperation(assertAllowedToWrite), NOTHING_ALLOWED)
        );

        assertThat(err.getMessage(), is("Thread %test%test is not allowed to do STORAGE_WRITE"));
    }

    private static void executeWithPermissions(Runnable task, ThreadOperation... allowedOperations) throws Throwable {
        AtomicReference<Throwable> exRef = new AtomicReference<>();

        Thread thread = new IgniteThread("test", "test", () -> {
            try {
                task.run();
            } catch (Throwable e) {
                exRef.set(e);
            }
        }, allowedOperations);

        thread.start();
        thread.join();

        if (exRef.get() != null) {
            if (exRef.get() instanceof AssertionError) {
                throw exRef.get();
            }

            throw new AssertionError(exRef.get());
        }
    }
}
