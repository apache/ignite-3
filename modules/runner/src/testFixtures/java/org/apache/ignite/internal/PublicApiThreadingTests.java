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

package org.apache.ignite.internal;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.schema.SchemaSyncInhibitor;
import org.apache.ignite.internal.thread.IgniteThread;
import org.hamcrest.Matcher;

/**
 * Common code for tests of Public API threading aspects.
 */
public class PublicApiThreadingTests {
    /**
     * Returns a {@link Matcher} that matches threads from {@link ForkJoinPool#commonPool()}.
     */
    public static Matcher<Object> asyncContinuationPool() {
        return both(hasProperty("name", startsWith("ForkJoinPool.commonPool-worker-")))
                .and(not(instanceOf(IgniteThread.class)));
    }

    /**
     * Returns a {@link Matcher} that matches instances of {@link IgniteThread}.
     */
    public static Matcher<Object> anIgniteThread() {
        return instanceOf(IgniteThread.class);
    }

    /**
     * Makes an action execution to happen in an Ignite internal thread. This is achieved by inhibiting metastorage
     * watches, then waiting for DelayDuration to pass. As a result, if a schema sync is made by the action,
     * the schema sync future will not be resolved right away; instead, it will be resolved in an Ignite thread.
     *
     * <p>This only works for actions that do schema sync.
     *
     * @param ignite Ignite node.
     * @param action Action to do.
     * @return Whatever the action returns.
     */
    public static <T> T tryToSwitchFromUserThreadWithDelayedSchemaSync(Ignite ignite, Supplier<? extends T> action) {
        return SchemaSyncInhibitor.withInhibition(ignite, action);
    }
}
