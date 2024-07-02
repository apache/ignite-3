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

package org.apache.ignite.internal.deployunit.exception;

import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.lang.ErrorGroups.CodeDeployment;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Throws when trying to access information about unit which doesn't exist.
 */
public class DeploymentUnitNotFoundException extends IgniteException {

    /**
     * Unit id.
     */
    private final String id;

    /**
     * Unit version.
     */
    private final Version version;

    /**
     * Constructor.
     *
     * @param id Unit id.
     */
    public DeploymentUnitNotFoundException(String id) {
        this(id, null);
    }


    /**
     * Constructor.
     *
     * @param id Unit id.
     * @param version Unit version.
     */
    public DeploymentUnitNotFoundException(String id, @Nullable Version version) {
        super(CodeDeployment.UNIT_NOT_FOUND_ERR, message(id, version));
        this.id = id;
        this.version = version;
    }

    private static String message(String id, @Nullable Version version) {
        if (version == null) {
            return "Unit " + id + " not found";
        } else {
            return "Unit " + id + ":" + version + " not found";
        }
    }

    public String id() {
        return id;
    }

    public Version version() {
        return version;
    }
}
