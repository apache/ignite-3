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

import java.util.Map;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

/**
 * Deploy unit request.
 */
@Transferable(DeployUnitMessageTypes.DEPLOY_UNIT_REQUEST)
public interface DeployUnitRequest extends NetworkMessage {
    /**
     * Returns id of deployment unit.
     *
     * @return id of deployment unit
     */
    String id();

    /**
     * Returns version of deployment unit.
     *
     * @return version of deployment unit.
     */
    String version();

    /**
     * Returns map from file names of deployment unit to their content.
     *
     * @return map from file names of deployment unit to their content.
     */

    Map<String, byte[]> unitContent();
}
