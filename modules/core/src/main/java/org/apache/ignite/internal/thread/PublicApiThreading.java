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

import java.util.function.Supplier;

/**
 * Logic related to the threading concern of Public APIs.
 */
public class PublicApiThreading {
    private static final ThreadLocal<Boolean> INTERNAL_CALL = new ThreadLocal<>();

    /**
     * Raises the 'internal call' state; the state is stored in the current thread.
     */
    public static void startInternalCall() {
        INTERNAL_CALL.set(true);
    }

    /**
     * Clears the 'internal call' state; the state is stored in the current thread.
     */
    public static void endInternalCall() {
        INTERNAL_CALL.remove();
    }

    /**
     * Executes the call while the 'internal call' state is raised; so if the call invokes {@link #inInternalCall()},
     * if will get {@code true}.
     *
     * @param call Call to execute as internal.
     * @return Call result.
     */
    public static <T> T doInternalCall(Supplier<T> call) {
        startInternalCall();

        try {
            return call.get();
        } finally {
            endInternalCall();
        }
    }

    /**
     * Returns {@code} true if the 'internal call' status is currently raised in the current thread.
     */
    public static boolean inInternalCall() {
        Boolean value = INTERNAL_CALL.get();
        return value != null && value;
    }
}
