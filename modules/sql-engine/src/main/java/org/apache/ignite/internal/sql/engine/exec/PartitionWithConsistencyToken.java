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

/**
 * Tuple representing the number of the partition with node enlistment consistency token.
 */
public class PartitionWithConsistencyToken {
    /** Partition number. */
    private final int partId;

    /** Enlistment consistency token. */
    private final long enlistmentConsistencyToken;

    /**
     * Constructor.
     *
     * @param partId partition number
     * @param enlistmentConsistencyToken Enlistment consistency token.
     */
    public PartitionWithConsistencyToken(int partId, Long enlistmentConsistencyToken) {
        this.partId = partId;
        this.enlistmentConsistencyToken = enlistmentConsistencyToken;
    }

    /**
     * Gets partition number.
     *
     * @return Partition number.
     */
    public int partId() {
        return partId;
    }

    /**
     * Gets enlistment consistency token.
     *
     * @return Enlistment consistency token.
     */
    public long enlistmentConsistencyToken() {
        return enlistmentConsistencyToken;
    }
}
