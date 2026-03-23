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

package org.apache.ignite.internal.deployunit.message;

import org.apache.ignite.internal.network.annotations.MessageGroup;

/**
 * Message group for deployment units.
 */
@MessageGroup(groupType = 10, groupName = "DeploymentUnit")
public class DeployUnitMessageTypes {
    /**
     * Message type for {@link DownloadUnitRequest}.
     */
    public static final short DOWNLOAD_UNIT_REQUEST = 0;

    /**
     * Message type for {@link DownloadUnitResponse}.
     */
    public static final short DOWNLOAD_UNIT_RESPONSE = 1;

    /**
     * Message type for {@link StopDeployRequest}.
     */
    public static final short STOP_DEPLOY_REQUEST = 2;

    /**
     * Message type for {@link StopDeployResponse}.
     */
    public static final short STOP_DEPLOY_RESPONSE = 3;
}
