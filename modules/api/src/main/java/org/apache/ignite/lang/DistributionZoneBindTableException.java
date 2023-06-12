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

package org.apache.ignite.lang;

import static org.apache.ignite.lang.ErrorGroups.DistributionZones.ZONE_DROP_ERR;

import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Exception is thrown when the distribution zone cannot be dropped because there is a table bound to the distribution zone.
 */
public class DistributionZoneBindTableException extends IgniteInternalException {
    /**
     * The constructor.
     *
     * @param zoneName Zone name.
     * @param tableName Table name.
     */
    public DistributionZoneBindTableException(String zoneName, String tableName) {
        this(zoneName, tableName, null);
    }

    /**
     * The constructor.
     *
     * @param zoneName Zone name.
     * @param tableName Table name.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public DistributionZoneBindTableException(String zoneName, String tableName, @Nullable Throwable cause) {
        super(ZONE_DROP_ERR, "Distribution zone is assigned to the table [zoneName=" + zoneName + ", tableName=" + tableName + ']',
                cause);
    }

    /**
     * The constructor is used for creating an exception instance that is thrown from a remote server.
     *
     * @param traceId Trace id.
     * @param code Error code.
     * @param message Error message.
     * @param cause Cause exception.
     */
    public DistributionZoneBindTableException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
