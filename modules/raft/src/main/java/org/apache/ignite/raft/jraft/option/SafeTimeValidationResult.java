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

package org.apache.ignite.raft.jraft.option;

/** Result of safe time validation by {@link SafeTimeValidator}. */
public class SafeTimeValidationResult {
    private static final SafeTimeValidationResult VALID = new SafeTimeValidationResult(true, false, "");

    private final boolean valid;
    private final boolean shouldRetry;
    private final String errorMessage;

    /** Returns valid result. */
    public static SafeTimeValidationResult forValid() {
        return VALID;
    }

    /** Returns result that requires a retry. */
    public static SafeTimeValidationResult forRetry(String errorMessage) {
        return new SafeTimeValidationResult(false, true, errorMessage);
    }

    /** Returns result saying that the ActionRequest is rejected for good. Such a request should never be retried by the Raft client. */
    public static SafeTimeValidationResult forRejected(String errorMessage) {
        return new SafeTimeValidationResult(false, false, errorMessage);
    }

    private SafeTimeValidationResult(boolean valid, boolean shouldRetry, String errorMessage) {
        this.valid = valid;
        this.shouldRetry = shouldRetry;
        this.errorMessage = errorMessage;
    }

    /** Returns whether the safe time and the corresponding command are valid. */
    public boolean valid() {
        return valid;
    }

    /** Returns whether the request should be retried or the request is rejected for good. */
    public boolean shouldRetry() {
        return shouldRetry;
    }

    /** Returns error message going with the failed result. */
    public String errorMessage() {
        return errorMessage;
    }
}
