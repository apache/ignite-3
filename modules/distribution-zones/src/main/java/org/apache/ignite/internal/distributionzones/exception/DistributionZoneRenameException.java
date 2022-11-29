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

package org.apache.ignite.internal.distributionzones.exception;

import static org.apache.ignite.lang.ErrorGroups.DistributionZones.ZONE_RENAME_ERR;

import java.util.UUID;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * This exception is thrown when a distribution zone with old name doesn't exist or a distribution zone with new name already exists.
 */
public class DistributionZoneRenameException extends IgniteInternalException {
    /**
     * The constructor.
     *
     * @param oldName Old name.
     * @param newName New name.
     */
    public DistributionZoneRenameException(String oldName, String newName, Throwable cause) {
        super(ZONE_RENAME_ERR, "Distribution zone with old name doesn't exist or "
                + "distribution zone with new name already exists [oldName=" + oldName + "newName=" + newName + ']', cause);
    }

    /**
     * The constructor is used for creating an exception instance that is thrown from a remote server.
     *
     * @param traceId Trace id.
     * @param code Error code.
     * @param message Error message.
     * @param cause Cause exception.
     */
    public DistributionZoneRenameException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
