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

package org.apache.ignite.internal.cli.core.call;

/**
 * Output of {@link Call}.
 *
 * @param <T> type of the body.
 */
public interface CallOutput<T> {
    /**
     * Body provider method.
     *
     * @return Body of the call's output. Can be {@link String} or any other type.
     */
    T body();

    /**
     * Error check method.
     *
     * @return True if output has an error.
     */
    boolean hasError();

    /**
     * Check if Call output is empty.
     *
     * @return true if call output is empty.
     */
    boolean isEmpty();

    /**
     * Exception cause provider method.
     *
     * @return the cause of the error.
     */
    Throwable errorCause();
}
