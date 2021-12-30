/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
 * Class that represents an intermediate result of the execution.
 *
 * @param <T> Intermediate result's type.
 */
class ExecutionResult<T> {
    /** Id of the node that sent result. */
    private final String nodeId;

    /** Compute task's intermediate result. */
    private final T result;

    /** Compute task error. */
    private final Throwable exception;

    ExecutionResult(String nodeId, T result, Throwable exception) {
        this.nodeId = nodeId;
        this.result = result;
        this.exception = exception;
    }

    String nodeId() {
        return nodeId;
    }

    T result() {
        return result;
    }

    Throwable exception() {
        return exception;
    }
}
