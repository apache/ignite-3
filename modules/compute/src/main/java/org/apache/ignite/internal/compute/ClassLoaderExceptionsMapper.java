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

import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCompletionThrowable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ignite.internal.compute.loader.JobContext;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitNotFoundException;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitUnavailableException;

class ClassLoaderExceptionsMapper {
    // <class_fqdn>. Deployment unit <deployment_unit_id_and ver> doesn't exist.
    private static final String DEPLOYMENT_UNIT_DOES_NOT_EXIST_MSG = "%s. Deployment unit %s:%s doesn't exist";

    // <class_fqdn>. Deployment unit <deployment_unit_id> can't be used:
    // [clusterStatus = <clusterDURecord.status>, nodeStatus = <nodeDURecord.status>].
    private static final String DEPLOYMENT_UNIT_NOT_AVAILABLE_MSG = "%s. Deployment unit %s:%s can't be used: "
            + "[clusterStatus = %s, nodeStatus = %s]";

    static CompletableFuture<JobContext> mapClassLoaderExceptions(
            CompletableFuture<JobContext> future,
            String jobClassName
    ) {
        return future.handle((v, e) -> {
            if (e instanceof Exception) {
                throw new CompletionException(mapException((Exception) unwrapCompletionThrowable((Exception) e), jobClassName));
            } else {
                return v;
            }
        });
    }

    private static Exception mapException(Exception e, String jobClassName) {
        if (e instanceof DeploymentUnitNotFoundException) {
            return new ClassNotFoundException(
                    String.format(
                            DEPLOYMENT_UNIT_DOES_NOT_EXIST_MSG,
                            jobClassName,
                            ((DeploymentUnitNotFoundException) e).id(),
                            ((DeploymentUnitNotFoundException) e).version()
                    )
            );
        } else if (e instanceof DeploymentUnitUnavailableException) {
            return new ClassNotFoundException(
                    String.format(
                            DEPLOYMENT_UNIT_NOT_AVAILABLE_MSG,
                            jobClassName,
                            ((DeploymentUnitUnavailableException) e).id(),
                            ((DeploymentUnitUnavailableException) e).version(),
                            ((DeploymentUnitUnavailableException) e).clusterStatus(),
                            ((DeploymentUnitUnavailableException) e).nodeStatus()
                    )
            );
        } else {
            return e;
        }
    }
}
