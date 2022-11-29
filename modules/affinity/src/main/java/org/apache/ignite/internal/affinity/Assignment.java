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

package org.apache.ignite.internal.affinity;

import java.io.Serializable;
import org.apache.ignite.internal.tostring.S;

/**
 * Represent an assignment of a partition to a node with a specific {@code consistentId}.
 *
 * <p>There can be two types of assignments: one for the synchronous members of a replication group (a.k.a. "peers") and one for
 * the asynchronous members (a.k.a. "learners") of the same group. Peers get synchronously updated during write operations, while learners
 * are eventually consistent and received updates some time in the future.
 */
public class Assignment implements Serializable {
    private static final long serialVersionUID = -8892379245627437834L;

    private final String consistentId;

    private final boolean isPeer;

    private Assignment(String consistentId, boolean isPeer) {
        this.consistentId = consistentId;
        this.isPeer = isPeer;
    }

    /**
     * Creates a peer assignment.
     *
     * @param consistentId Peer consistent ID.
     */
    public static Assignment forPeer(String consistentId) {
        return new Assignment(consistentId, true);
    }

    /**
     * Creates a learner assignment.
     *
     * @param consistentId Learner consistent ID.
     */
    public static Assignment forLearner(String consistentId) {
        return new Assignment(consistentId, false);
    }

    public String consistentId() {
        return consistentId;
    }

    public boolean isPeer() {
        return isPeer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Assignment that = (Assignment) o;

        if (isPeer != that.isPeer) {
            return false;
        }
        return consistentId.equals(that.consistentId);
    }

    @Override
    public int hashCode() {
        int result = consistentId.hashCode();
        result = 31 * result + (isPeer ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return S.toString(Assignment.class, this);
    }
}
