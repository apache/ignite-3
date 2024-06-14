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

import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.compute.ComputeMessageTypes;
import org.apache.ignite.internal.compute.ExecutionOptions;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/**
 * Used to implement remote job execution in {@link org.apache.ignite.compute.IgniteCompute#execute(Set, List, String, Object...)}.
 */
@Transferable(value = ComputeMessageTypes.EXECUTE_REQUEST)
public interface ExecuteRequest extends NetworkMessage {
    /**
     * Returns job execution options.
     *
     * @return Job execution options.
     */
    @Marshallable
    ExecutionOptions executeOptions();

    /**
     * Returns list of deployment units.
     *
     * @return list of deployment units
     */
    List<DeploymentUnitMsg> deploymentUnits();

    /**
     * Returns job class name.
     *
     * @return job class name
     */
    String jobClassName();

    /**
     * Returns job arguments.
     *
     * @return arguments
     */
    @Marshallable
    @Nullable
    Object input();
}
