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

package org.apache.ignite.lang;

/**
 * Cancellation token is an object that is issued by {@link CancelHandle} and can be used by an operation or a resource to observe a signal
 * to terminate it.
 */
public interface CancellationToken {
    /**
     * Flag indicating whether cancellation was requested or not.
     *
     * <p>This method will return {@code true} even if cancellation has not been completed yet.
     *
     * @return {@code true} when cancellation was requested.
     */
    boolean isCancelled();

    /**
     * Registers a callback to be executed when cancellation is requested. If cancellation has already been requested,
     * the callback is executed immediately.
     *
     * <p>The returned handle can be used to stop listening for cancellation requests. It is important to close the handle
     * when the callback is no longer needed to avoid memory leaks.
     *
     * @param callback Callback to execute when cancellation is requested.
     * @return A handle which can be used to stop listening for cancellation requests.
     */
    AutoCloseable addListener(Runnable callback);
}
