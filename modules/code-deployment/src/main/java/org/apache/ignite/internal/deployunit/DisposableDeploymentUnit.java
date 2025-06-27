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

package org.apache.ignite.internal.deployunit;

import java.nio.file.Path;
import java.util.Objects;
import org.apache.ignite.deployment.DeploymentUnit;

/**
 * Disposable deployment unit. This class is used to track deployment units that are not needed any more.
 * When the unit is not needed any more, {@link #release()} method should be called to release resources.
 */
public class DisposableDeploymentUnit {
    private final DeploymentUnit unit;

    private final Path path;

    private final Runnable release;

    /**
     * Constructor.
     *
     * @param unit Deployment unit.
     * @param path Path to the deployment unit.
     * @param release Release. This runnable will be called when the unit is not needed any more.
     */
    public DisposableDeploymentUnit(DeploymentUnit unit, Path path, Runnable release) {
        this.unit = unit;
        this.path = path;
        this.release = release;
    }

    /**
     * Returns deployment unit.
     *
     * @return Deployment unit.
     */
    public DeploymentUnit unit() {
        return unit;
    }

    /**
     * Returns path to the deployment unit.
     *
     * @return Path to the deployment unit.
     */
    public Path path() {
        return path;
    }

    /**
     * Releases resources of the unit. This method should be called when the unit is not needed any more.
     */
    public void release() {
        release.run();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DisposableDeploymentUnit that = (DisposableDeploymentUnit) o;
        return Objects.equals(unit, that.unit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(unit);
    }

    @Override
    public String toString() {
        return "DisposableDeploymentUnit{"
                + "unit=" + unit
                + ", path=" + path + '}';
    }
}
