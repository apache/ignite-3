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

package org.apache.ignite.internal.metastorage.exceptions;

import static org.apache.ignite.lang.ErrorGroups.MetaStorage.COMPACTED_ERR;

/**
 * Thrown when a requested read operation on meta storage could not be performed because target revisions were removed from storage due to
 * a compaction procedure. In such case the operation should be retried with actual revision.
 */
public class CompactedException extends MetaStorageException {
    private static final long serialVersionUID = -6849399873850280288L;

    /**
     * Constructs an exception.
     */
    public CompactedException() {
        super(COMPACTED_ERR);
    }

    /**
     * Constructs an exception with a given message.
     *
     * @param requestedRevision Requested revision.
     * @param latestCompactedRevision Latest compacted revision.
     */
    public CompactedException(long requestedRevision, long latestCompactedRevision) {
        super(
                COMPACTED_ERR,
                String.format(
                        "Requested revision has already been compacted: [requested=%s, lastCompacted=%s]",
                        requestedRevision, latestCompactedRevision
                )
        );
    }

    /**
     * Constructs an exception with a given message.
     *
     * @param message Detail message.
     */
    public CompactedException(String message) {
        super(COMPACTED_ERR, message);
    }

    /**
     * Constructs an exception with a given message and a cause.
     *
     * @param message Detail message.
     * @param cause   Cause.
     */
    public CompactedException(String message, Throwable cause) {
        super(COMPACTED_ERR, message, cause);
    }

    /**
     * Constructs an exception with a given cause.
     *
     * @param cause Cause.
     */
    public CompactedException(Throwable cause) {
        super(COMPACTED_ERR, cause);
    }

    /** Throws {@link CompactedException} if the requested revision is less than or equal to the last compacted one. */
    public static void throwIfRequestedRevisionLessThanOrEqualToCompacted(long requestedRevision, long compactedRevision) {
        if (requestedRevision <= compactedRevision) {
            throw new CompactedException(requestedRevision, compactedRevision);
        }
    }
}
