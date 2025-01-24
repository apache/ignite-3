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

import java.util.List;
import org.apache.ignite.deployment.DeploymentUnit;
import org.jetbrains.annotations.Nullable;

/**
 * Captures the context of a remote job execution.
 */
class RemoteExecutionContext {
    private final ExecutionOptions executionOptions;

    private final List<DeploymentUnit> units;

    private final String jobClassName;

    private final ComputeJobDataHolder arg;

    RemoteExecutionContext(
            List<DeploymentUnit> units,
            String jobClassName,
            ExecutionOptions executionOptions,
            @Nullable ComputeJobDataHolder arg
    ) {
        this.executionOptions = executionOptions;
        this.units = units;
        this.jobClassName = jobClassName;
        this.arg = arg;
    }

    ExecutionOptions executionOptions() {
        return executionOptions;
    }

    List<DeploymentUnit> units() {
        return units;
    }

    String jobClassName() {
        return jobClassName;
    }

    @Nullable ComputeJobDataHolder arg() {
        return arg;
    }
}
