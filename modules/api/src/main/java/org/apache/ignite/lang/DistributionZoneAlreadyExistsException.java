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

import static org.apache.ignite.lang.ErrorGroups.DistributionZones.ZONE_ALREADY_EXISTS_ERR;

import java.util.UUID;

/**
 * This exception is thrown when a new distribution zone failed to be created,
 * because a distribution zone with same name already exists.
 */
public class DistributionZoneAlreadyExistsException extends IgniteInternalException {
    /**
     * The constructor.
     *
     * @param zoneName Zone name.
     */
    public DistributionZoneAlreadyExistsException(String zoneName) {
        this(zoneName, null);
    }

    /**
     * The constructor.
     *
     * @param zoneName Zone name.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public DistributionZoneAlreadyExistsException(String zoneName, Throwable cause) {
        super(ZONE_ALREADY_EXISTS_ERR, "Distribution zone already exists [zoneName=" + zoneName + ']', cause);
    }

    /**
     * The constructor is used for creating an exception instance that is thrown from a remote server.
     *
     * @param traceId Trace id.
     * @param code Error code.
     * @param message Error message.
     * @param cause Cause exception.
     */
    public DistributionZoneAlreadyExistsException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
