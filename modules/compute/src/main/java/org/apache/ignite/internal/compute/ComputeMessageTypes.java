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

package org.apache.ignite.internal.compute;

import org.apache.ignite.internal.compute.message.DeploymentUnitMsg;
import org.apache.ignite.internal.compute.message.ExecuteRequest;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Message types for the Compute module.
 */
@MessageGroup(groupType = 6, groupName = "ComputeMessages")
public class ComputeMessageTypes {
    /**
     * Type for {@link ExecuteRequest}.
     */
    public static final short EXECUTE_REQUEST = 0;

    /**
     * Type for {@link ExecuteResponse}.
     */
    public static final short EXECUTE_RESPONSE = 1;

    /**
     * Type for {@link DeploymentUnitMsg}.
     */
    public static final short DEPLOYMENT_UNIT = 2;
}
