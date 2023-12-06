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

package org.apache.ignite.internal.client.proto;

/**
 * Response flags utils.
 */
public class ResponseFlags {
    /** Partitions assignment flag. */
    private static final int PARTITION_ASSIGNMENT_FLAG = 1;

    /** Notification flag. */
    private static final int NOTIFICATION_FLAG = 2;

    /** Error flag. */
    private static final int ERROR_FLAG = 4;

    /**
     * Gets flags as int.
     *
     * @param partitionAssignmentChanged Assignment changed flag.
     * @return Flags as int.
     */
    public static int getFlags(boolean partitionAssignmentChanged, boolean isNotification, boolean hasError) {
        var flags = 0;

        if (partitionAssignmentChanged) {
            flags |= PARTITION_ASSIGNMENT_FLAG;
        }

        if (isNotification) {
            flags |= NOTIFICATION_FLAG;
        }

        if (hasError) {
            flags |= ERROR_FLAG;
        }

        return flags;
    }

    /**
     * Gets partition assignment flag value.
     *
     * @param flags Flags.
     * @return Whether partition assignment has changed.
     */
    public static boolean getPartitionAssignmentChangedFlag(int flags) {
        return (flags & PARTITION_ASSIGNMENT_FLAG) == PARTITION_ASSIGNMENT_FLAG;
    }

    /**
     * Gets notification flag value.
     *
     * @param flags Flags.
     * @return Whether notification is present.
     */
    public static boolean getNotificationFlag(int flags) {
        return (flags & NOTIFICATION_FLAG) == NOTIFICATION_FLAG;
    }

    /**
     * Gets error flag value.
     *
     * @param flags Flags.
     * @return Whether error is present.
     */
    public static boolean getErrorFlag(int flags) {
        return (flags & ERROR_FLAG) == ERROR_FLAG;
    }
}
