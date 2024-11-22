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

package org.apache.ignite.internal.tx.views;

import static org.apache.ignite.internal.type.NativeTypes.stringOf;

import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.tx.Lock;
import org.apache.ignite.internal.tx.LockKey;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.util.SubscriptionUtils;

/**
 * {@code LOCKS} system view provider.
 */
public class LocksViewProvider {
    /** Active locks. */
    private final Iterable<Lock> locks;

    public LocksViewProvider(Iterable<Lock> locks) {
        this.locks = locks;
    }

    /** Returns system view exposing active locks. */
    public SystemView<?> get() {
        NativeType stringType = stringOf(Short.MAX_VALUE);

        return SystemViews.<Lock>nodeViewBuilder()
                .name("LOCKS")
                .nodeNameColumnAlias("OWNING_NODE_ID")
                .<String>addColumn("TX_ID", stringType, lock -> lock.txId().toString())
                .<String>addColumn("OBJECT_ID", stringType, lock -> formatLockKey(lock.lockKey()))
                .<String>addColumn("MODE", stringType, lock -> lock.lockMode().name())
                .dataProvider(SubscriptionUtils.fromIterable(locks))
                .build();
    }

    private String formatLockKey(LockKey lockKey) {
        return lockKey.toString();
    }
}
