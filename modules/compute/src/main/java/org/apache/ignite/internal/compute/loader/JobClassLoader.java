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

package org.apache.ignite.internal.compute.loader;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.lang.IgniteException;

/**
 * Implementation of {@link ClassLoader} that loads classes from specified component directories.
 */
public class JobClassLoader implements AutoCloseable {
    private final List<DisposableDeploymentUnit> units;

    private final ClassLoader parent;

    private ClassLoader inner;

    /**
     * Creates new instance of {@link JobClassLoader}.
     *
     * @param urls URLs to load classes from.
     * @param units Units to load classes from.
     * @param parent Parent class loader.
     */
    public JobClassLoader(List<DisposableDeploymentUnit> units, URL[] urls, ClassLoader parent) {
        this.units = units;
        this.parent = parent;
    }

    public List<DisposableDeploymentUnit> units() {
        return units;
    }

    public ClassLoader classLoader() {
        // TODO: Init class URLs lazily.
        return null;
    }

    @Override
    public void close() {
        List<Exception> exceptions = new ArrayList<>();

        for (DisposableDeploymentUnit unit : units) {
            try {
                unit.release();
            } catch (Exception e) {
                exceptions.add(e);
            }
        }

        try {
            // TODO: Close wrapped class loader if any.
            // super.close();
        } catch (Exception e) {
            exceptions.add(e);
        }

        if (!exceptions.isEmpty()) {
            IgniteException igniteException = new IgniteException(
                    Compute.CLASS_LOADER_ERR,
                    "Failed to close class loader"
            );

            for (Exception exception : exceptions) {
                igniteException.addSuppressed(exception);
            }
            throw igniteException;
        }
    }
}
