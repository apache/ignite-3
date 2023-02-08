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


import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Message group for deployment units.
 */
@MessageGroup(groupType = 10, groupName = "DeploymentUnit")
public class DeployUnitMessageTypes {

    /**
     * Message for {@link DeployUnitRequest}.
     */
    public static final short DEPLOY_UNIT_REQUEST = 0;

    /**
     * Message for {@link DeployUnitResponse}.
     */
    public static final short DEPLOY_UNIT_RESPONSE = 1;

    /**
     * Message for {@link UndeployUnitRequest}.
     */
    public static final short UNDEPLOY_UNIT_REQUEST = 2;

    /**
     * Message for {@link UndeployUnitResponse}.
     */
    public static final short UNDEPLOY_UNIT_RESPONSE = 3;
}
