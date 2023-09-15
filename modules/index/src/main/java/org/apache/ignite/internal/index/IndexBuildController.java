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

package org.apache.ignite.internal.index;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.index.event.IndexEvent;
import org.apache.ignite.internal.index.event.IndexEventParameters;
import org.apache.ignite.internal.table.distributed.index.IndexBuilder;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/** No doc. */
// TODO: IGNITE-20330 документация и тесты
public class IndexBuildController implements ManuallyCloseable {
    private final IndexBuilder indexBuilder;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Constructor. */
    public IndexBuildController(IndexManager indexManager, IndexBuilder indexBuilder) {
        this.indexBuilder = indexBuilder;

        indexManager.listen(IndexEvent.CREATE, (parameters, exception) -> inBusyLockAsync(busyLock, () -> {
            onIndexCreate(parameters);

            return completedFuture(false);
        }));

        indexManager.listen(IndexEvent.DROP, (parameters, exception) -> inBusyLockAsync(busyLock, () -> {
            onIndexDrop(parameters);

            return completedFuture(false);
        }));
    }

    private synchronized void onIndexCreate(IndexEventParameters parameters) {
        // TODO: IGNITE-20330 написать код
    }

    private synchronized void onIndexDrop(IndexEventParameters parameters) {
        // TODO: IGNITE-20330 написать код
    }

    @Override
    public void close() throws Exception {
        if (!closeGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }
}
