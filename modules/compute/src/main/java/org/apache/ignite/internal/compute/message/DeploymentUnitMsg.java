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

package org.apache.ignite.internal.compute.message;

import java.io.Serializable;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.internal.compute.ComputeMessageTypes;
import org.apache.ignite.internal.compute.ComputeMessagesFactory;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Deployment unit message.
 */
@Transferable(value = ComputeMessageTypes.DEPLOYMENT_UNIT)
public interface DeploymentUnitMsg extends NetworkMessage, Serializable {

    /**
     * Returns deployment unit name.
     *
     * @return Deployment unit name.
     */
    String name();

    /**
     * Returns deployment unit version.
     *
     * @return Deployment unit version.
     */
    String version();

    /**
     * Creates a new deployment unit message.
     *
     * @param factory Message factory.
     * @param unit Deployment unit.
     * @return Deployment unit message.
     */
    static DeploymentUnitMsg fromDeploymentUnit(ComputeMessagesFactory factory, DeploymentUnit unit) {
        return factory.deploymentUnitMsg()
                .name(unit.name())
                .version(unit.version().toString())
                .build();
    }
}
