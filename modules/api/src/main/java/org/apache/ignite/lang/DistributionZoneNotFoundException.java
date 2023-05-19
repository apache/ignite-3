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

import static org.apache.ignite.lang.ErrorGroups.DistributionZones.ZONE_NOT_FOUND_ERR;

import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Exception is thrown when appropriate distribution zone can`t be found.
 */
public class DistributionZoneNotFoundException extends IgniteInternalException {
    /**
     * The constructor.
     *
     * @param zoneName Zone name.
     */
    public DistributionZoneNotFoundException(String zoneName) {
        this(zoneName, null);
    }

    /**
     * The constructor.
     *
     * @param zoneId Zone id.
     */
    public DistributionZoneNotFoundException(int zoneId) {
        super(ZONE_NOT_FOUND_ERR, "Distribution zone is not found [zoneId=" + zoneId + ']');
    }

    /**
     * The constructor.
     *
     * @param zoneName Zone name.
     * @param cause Optional nested exception (can be {@code null}).
     */
    public DistributionZoneNotFoundException(String zoneName, @Nullable Throwable cause) {
        super(ZONE_NOT_FOUND_ERR, "Distribution zone is not found [zoneName=" + zoneName + ']', cause);
    }

    /**
     * The constructor is used for creating an exception instance that is thrown from a remote server.
     *
     * @param traceId Trace id.
     * @param code Error code.
     * @param message Error message.
     * @param cause Cause exception.
     */
    public DistributionZoneNotFoundException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
