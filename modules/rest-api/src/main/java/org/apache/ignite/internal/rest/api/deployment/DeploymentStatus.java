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

package org.apache.ignite.internal.rest.api.deployment;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Status of deployment process.
 */
@Schema
public enum DeploymentStatus {
    /** Unit deployment is in progress. */
    UPLOADING,

    /** Unit is deployed on the cluster. */
    DEPLOYED,

    /** Remove command was initiated for the unit and it will be removed soon. */
    OBSOLETE,

    /** Unit removal from the cluster is in progress. */
    REMOVING
}
