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


package org.apache.ignite.internal.tx.impl;

import java.util.Comparator;
import java.util.UUID;
import org.apache.ignite.internal.tx.DeadlockPreventionPolicy;

/**
 * Deadlock prevention policy that, in case of conflict of transactions tx1 and tx2 on the same key, assuming tx1 is holding the lock,
 * allows tx2 to wait for the lock if tx2 is older than tx1, and aborts tx2 is tx2 is younger than tx1.
 */
public class WaitDieDeadlockPreventionPolicy implements DeadlockPreventionPolicy {
    @Override
    public Comparator<UUID> txIdComparator() {
        return UUID::compareTo;
    }
}
