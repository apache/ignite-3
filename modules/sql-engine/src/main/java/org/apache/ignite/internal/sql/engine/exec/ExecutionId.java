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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.tostring.S;

/**
 * Unique identity of distributed plan execution.
 * 
 * <p>Includes a token to separate retries within a single query.
 */
public class ExecutionId {
    private final UUID queryId;
    private final int executionToken;

    public ExecutionId(UUID queryId, int executionToken) {
        this.queryId = queryId;
        this.executionToken = executionToken;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExecutionId that = (ExecutionId) o;
        return executionToken == that.executionToken && Objects.equals(queryId, that.queryId);
    }

    public UUID queryId() {
        return queryId;
    }

    public int executionToken() {
        return executionToken;
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryId, executionToken);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
