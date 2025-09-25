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

package org.apache.ignite.compute;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.deployment.DeploymentUnitInfo;
import org.apache.ignite.table.partition.Partition;
import org.jetbrains.annotations.Nullable;

/**
 * Context of the {@link ComputeJob} execution.
 */
public interface JobExecutionContext {
    /**
     * Ignite API entry point.
     *
     * @return Ignite instance.
     */
    Ignite ignite();

    /**
     * Flag indicating whether the job was cancelled.
     *
     * @return {@code true} when the job was cancelled.
     */
    boolean isCancelled();

    /**
     * Partition associated with this job. Not a {@code null} only when {@link BroadcastJobTarget#table(String)} target is used for
     * submitting jobs. In this case, the partition is local on a node executing the job.
     *
     * @return Partition associated with this job.
     */
    @Nullable Partition partition();

    /**
     * Collection of deployment units associated with this job execution.
     *
     * @return Collection of deployment units for this job execution.
     */
    Collection<DeploymentUnitInfo> deploymentUnits();
}
