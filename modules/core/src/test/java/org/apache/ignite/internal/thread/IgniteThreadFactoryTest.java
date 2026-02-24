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

package org.apache.ignite.internal.thread;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;

import java.util.Set;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.junit.jupiter.api.Test;

class IgniteThreadFactoryTest {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteThreadFactoryTest.class);

    @Test
    void producesCorrectThreadNames() throws Exception {
        ThreadFactory threadFactory = IgniteThreadFactory.create("nodeName", "poolName", LOG);

        AtomicReference<String> threadName1Ref = new AtomicReference<>();
        AtomicReference<String> threadName2Ref = new AtomicReference<>();

        Thread thread1 = threadFactory.newThread(() -> threadName1Ref.set(Thread.currentThread().getName()));
        thread1.start();
        thread1.join(SECONDS.toMillis(10));
        Thread thread2 = threadFactory.newThread(() -> threadName2Ref.set(Thread.currentThread().getName()));
        thread2.start();
        thread2.join(SECONDS.toMillis(10));

        assertThat(threadName1Ref.get(), is("%nodeName%poolName-0"));
        assertThat(threadName2Ref.get(), is("%nodeName%poolName-1"));
    }

    @Test
    void producesRequestedAllowedOperations() throws Exception {
        ThreadFactory threadFactory = IgniteThreadFactory.create("nodeName", "poolName", LOG, STORAGE_WRITE);

        AtomicReference<Set<ThreadOperation>> allowedOperationsRef = new AtomicReference<>();

        Thread thread = threadFactory.newThread(() -> {
            allowedOperationsRef.set(((ThreadAttributes) Thread.currentThread()).allowedOperations());
        });
        thread.start();
        thread.join(SECONDS.toMillis(10));

        assertThat(allowedOperationsRef.get(), contains(STORAGE_WRITE));
    }
}
