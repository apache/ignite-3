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

package org.example.jobs.standalone;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.deployment.DeploymentUnit;

/**
 * Compute job that submits a nested {@link CancelAwareSleepJob} using the context's cancellation token.
 * When this job is canceled, the token propagates cancellation to the inner job.
 */
public class NestedSleepJob implements ComputeJob<Long, Void> {
    @Override
    public CompletableFuture<Void> executeAsync(JobExecutionContext context, Long timeout) {
        List<DeploymentUnit> units = context.deploymentUnits().stream()
                .map(info -> new DeploymentUnit(info.name(), info.version()))
                .collect(Collectors.toList());

        return context.ignite().compute().executeAsync(
                JobTarget.anyNode(context.ignite().clusterNodes()),
                JobDescriptor.builder(CancelAwareSleepJob.class).units(units).build(),
                timeout,
                context.cancellationToken()
        );
    }
}
