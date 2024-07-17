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

package org.apache.ignite.internal.raft;

/**
 * Represents a status of a Raft operation (either successful or not).
 */
public class Status {
    public static final Status LEADER_STEPPED_DOWN = new Status(RaftError.EPERM, "Leader stepped down.");

    private final RaftError error;

    private final String errorMessage;

    public Status(RaftError error, String errorMessage) {
        this.error = error;
        this.errorMessage = errorMessage;
    }

    public RaftError error() {
        return error;
    }

    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Status status = (Status) o;

        if (error != status.error) {
            return false;
        }
        return errorMessage.equals(status.errorMessage);
    }

    @Override
    public int hashCode() {
        int result = error.hashCode();
        result = 31 * result + errorMessage.hashCode();
        return result;
    }
}
