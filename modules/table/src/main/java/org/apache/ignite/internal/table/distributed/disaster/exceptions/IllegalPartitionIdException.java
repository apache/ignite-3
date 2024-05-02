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

package org.apache.ignite.internal.table.distributed.disaster.exceptions;

import static org.apache.ignite.lang.ErrorGroups.DisasterRecovery.ILLEGAL_PARTITION_ID_ERR;

/** Exception is thrown when illegal partition was requested. */
public class IllegalPartitionIdException extends DisasterRecoveryException {
    private static final long serialVersionUID = -9215416423159317425L;

    /** Creates exception that partition ID is negative. */
    public IllegalPartitionIdException(int partitionId) {
        super(ILLEGAL_PARTITION_ID_ERR, "Partition ID can't be negative, found: " + partitionId);
    }

    /** Creates exception that partition ID is bigger that partition count for zone. */
    public IllegalPartitionIdException(int partitionId, int partitions, String zoneName) {
        super(
                ILLEGAL_PARTITION_ID_ERR,
                String.format("Partition IDs should be in range [0, %d] for zone %s, found: %d", partitions - 1, zoneName, partitionId)
        );
    }
}
