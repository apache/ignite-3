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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.lang.ErrorGroups.Compute.CLASS_INITIALIZATION_ERR;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.compute.message.DeploymentUnitMsg;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteException;

/**
 * Utility class for compute.
 */
public class ComputeUtils {
    private static final ComputeMessagesFactory MESSAGES_FACTORY = new ComputeMessagesFactory();

    /**
     * Instantiate compute job via provided class loader by provided job class name.
     *
     * @param computeJobClass Compute job class.
     * @param <R> Compute job return type.
     * @return Compute job instance.
     */
    public static <R> ComputeJob<R> instantiateJob(Class<? extends ComputeJob<R>> computeJobClass) {
        if (!(ComputeJob.class.isAssignableFrom(computeJobClass))) {
            throw new IgniteException(
                    CLASS_INITIALIZATION_ERR,
                    "'" + computeJobClass.getName() + "' does not implement ComputeJob interface"
            );
        }

        try {
            Constructor<? extends ComputeJob<R>> constructor = computeJobClass.getDeclaredConstructor();

            if (!constructor.canAccess(null)) {
                constructor.setAccessible(true);
            }

            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IgniteInternalException(
                    CLASS_INITIALIZATION_ERR,
                    "Cannot instantiate job",
                    e
            );
        }
    }

    /**
     * Resolve compute job class name to compute job class reference.
     *
     * @param jobClassLoader Class loader.
     * @param jobClassName Job class name.
     * @param <R> Compute job return type.
     * @return Compute job class.
     */
    public static <R> Class<ComputeJob<R>> jobClass(ClassLoader jobClassLoader, String jobClassName) {
        try {
            return (Class<ComputeJob<R>>) Class.forName(jobClassName, true, jobClassLoader);
        } catch (ClassNotFoundException e) {
            throw new IgniteInternalException(
                    CLASS_INITIALIZATION_ERR,
                    "Cannot load job class by name '" + jobClassName + "'",
                    e
            );
        }
    }

    /**
     * Transform deployment unit object to message {@link DeploymentUnitMsg}.
     *
     * @param unit Deployment unit.
     * @return Deployment unit message.
     */
    public static DeploymentUnitMsg toDeploymentUnitMsg(DeploymentUnit unit) {
        return MESSAGES_FACTORY.deploymentUnitMsg()
                .name(unit.name())
                .version(unit.version().toString())
                .build();
    }

    /**
     * Extract Compute job result from execute response.
     *
     * @param executeResponse Execution message response.
     * @param <R> Compute job return type.
     * @return Completable future with result.
     */
    public static <R> CompletableFuture<R> resultFromExecuteResponse(ExecuteResponse executeResponse) {
        Throwable throwable = executeResponse.throwable();
        if (throwable != null) {
            return failedFuture(throwable);
        }

        return completedFuture((R) executeResponse.result());
    }

    /**
     * Transform list of deployment unit messages to list of deployment units.
     *
     * @param unitMsgs Deployment units messages.
     * @return Deployment units.
     */
    public static List<DeploymentUnit> toDeploymentUnit(List<DeploymentUnitMsg> unitMsgs) {
        return unitMsgs.stream()
                .map(it -> new DeploymentUnit(it.name(), Version.parseVersion(it.version())))
                .collect(Collectors.toList());
    }
}
