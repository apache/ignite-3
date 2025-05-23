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

package org.apache.ignite.client;

/**
 * Client retry policy determines whether client operations that have failed due to a connection issue should be retried.
 *
 * <p>Predefined policies: {@link RetryLimitPolicy}, {@link RetryReadPolicy}.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface RetryPolicy {
    /**
     * Gets a value indicating whether a client operation that has failed due to a connection issue should be retried.
     *
     * <p>If an implementation of this method throws an exception, the operation will be failed with that exception.
     * No more retries will be attempted.
     *
     * @param context Context.
     * @return {@code true} if the operation should be retried on another connection, {@code false} otherwise.
     */
    boolean shouldRetry(RetryPolicyContext context);
}
