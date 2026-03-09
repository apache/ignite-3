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

package org.apache.ignite.internal.app.di;

import static org.apache.ignite.internal.configuration.IgnitePaths.cmgPath;
import static org.apache.ignite.internal.configuration.IgnitePaths.metastoragePath;
import static org.apache.ignite.internal.configuration.IgnitePaths.partitionsPath;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.components.NodeIdentity;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;

/**
 * Micronaut factory for component working directories.
 */
@Factory
public class ComponentWorkingDirFactory {
    /** Creates the partitions working directory. */
    @Singleton
    @Named("partitions")
    public ComponentWorkingDir partitionsWorkDir(SystemLocalConfiguration systemConfiguration, NodeIdentity nodeIdentity) {
        return partitionsPath(systemConfiguration, nodeIdentity.workDir());
    }

    /** Creates the metastorage working directory. */
    @Singleton
    @Named("metastorage")
    public ComponentWorkingDir metastorageWorkDir(SystemLocalConfiguration systemConfiguration, NodeIdentity nodeIdentity) {
        return metastoragePath(systemConfiguration, nodeIdentity.workDir());
    }

    /** Creates the CMG working directory. */
    @Singleton
    @Named("cmg")
    public ComponentWorkingDir cmgWorkDir(SystemLocalConfiguration systemConfiguration, NodeIdentity nodeIdentity) {
        return cmgPath(systemConfiguration, nodeIdentity.workDir());
    }
}
