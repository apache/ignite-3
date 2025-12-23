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

package org.apache.ignite.internal.deployunit.loader;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.deployunit.loader.ClassLoaderExceptionsMapper.mapClassLoaderExceptions;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitUnavailableException;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

class ClassLoaderExceptionsMapperTest {
    @Test
    public void mapDeploymentUnitNotFoundException() {
        DeploymentUnitNotFoundException toBeThrown = new DeploymentUnitNotFoundException("id", Version.parseVersion("1.0.0"));
        assertThat(
                mapClassLoaderExceptions(failedFuture(toBeThrown), "com.example.Main"),
                willThrow(
                        ClassNotFoundException.class,
                        "com.example.Main. Deployment unit id:1.0.0 doesn't exist"
                )
        );
    }

    @Test
    public void mapDeploymentUnitUnavailableException() {
        DeploymentUnitUnavailableException toBeThrown = new DeploymentUnitUnavailableException(
                "id",
                Version.parseVersion("1.0.0"),
                DeploymentStatus.OBSOLETE,
                DeploymentStatus.REMOVING
        );

        assertThat(
                mapClassLoaderExceptions(failedFuture(toBeThrown), "com.example.Main"),
                willThrow(
                        ClassNotFoundException.class,
                        "com.example.Main. Deployment unit id:1.0.0 can't be used:"
                                + " [clusterStatus = OBSOLETE, nodeStatus = REMOVING]"
                )
        );
    }

    @Test
    void mapException() {
        IgniteException toBeThrown = new IgniteException(Compute.CLASS_LOADER_ERR, "Expected exception");
        assertThat(
                mapClassLoaderExceptions(failedFuture(toBeThrown), "com.example.Main"),
                willThrow(
                        toBeThrown.getClass(),
                        toBeThrown.getMessage()
                )
        );
    }

}
