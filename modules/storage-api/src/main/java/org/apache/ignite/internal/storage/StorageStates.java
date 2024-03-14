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

package org.apache.ignite.internal.storage;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.storage.util.StorageState;

/**
 * Utilities to work with storage states.
 */
public class StorageStates {
    /**
     * If not already in a terminal state, transitions to the supplied state and returns {@code true}, otherwise just returns {@code false}.
     */
    public static boolean transitionToTerminalState(StorageState targetState, AtomicReference<StorageState> stateRef) {
        assert targetState.isTerminal() : "Not a terminal state: " + targetState;

        while (true) {
            StorageState previous = stateRef.get();

            if (previous.isTerminal()) {
                return false;
            }

            if (stateRef.compareAndSet(previous, targetState)) {
                return true;
            }
        }
    }
}
