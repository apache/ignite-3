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

package org.apache.ignite.internal.replicator;

/**
 * Represents a member of a replication group.
 */
public class Member {
    private final String consistentId;

    private final boolean isVotingMember;

    /**
     * Constructor.
     *
     * @param consistentId Member's consistent ID.
     * @param isVotingMember Flag indicating whether this member is a voting member.
     */
    public Member(String consistentId, boolean isVotingMember) {
        this.consistentId = consistentId;
        this.isVotingMember = isVotingMember;
    }

    public static Member votingMember(String consistentId) {
        return new Member(consistentId, true);
    }

    public static Member learner(String consistentId) {
        return new Member(consistentId, false);
    }

    public String consistentId() {
        return consistentId;
    }

    public boolean isVotingMember() {
        return isVotingMember;
    }
}
