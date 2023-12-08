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

package org.apache.ignite.internal.compute;

/**
 * Compute job execution options.
 */
public class ExecutionOptions {
    public static final ExecutionOptions DEFAULT = new ExecutionOptions(0, 0);

    private final int priority;

    private final int maxRetries;

    /**
     * Constructor.
     *
     * @param priority Job execution priority.
     * @param maxRetries Number of times to retry job execution in case of failure, 0 to not retry.
     */
    public ExecutionOptions(int priority, int maxRetries) {
        this.priority = priority;
        this.maxRetries = maxRetries;
    }

    public int priority() {
        return priority;
    }

    public int maxRetries() {
        return maxRetries;
    }
}
