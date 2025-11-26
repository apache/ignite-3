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
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.lang.IgniteException;

/**
 * Implementation of {@link ClassLoader} that loads classes from specified component directories.
 */
public class JobClassLoader implements AutoCloseable {
    private static final IgniteLogger LOG = Loggers.forClass(JobClassLoader.class);

    private final List<DisposableDeploymentUnit> units;

    private final ClassLoader parent;

    private volatile JobClassLoaderImpl impl;

    /**
     * Creates new instance of {@link JobClassLoader}.
     *
     * @param units Units to load classes from.
     * @param parent Parent class loader.
     */
    public JobClassLoader(List<DisposableDeploymentUnit> units, ClassLoader parent) {
        this.units = units;
        this.parent = parent;
    }

    /**
     * Returns the list of deployment units.
     *
     * @return List of deployment units.
     */
    public List<DisposableDeploymentUnit> units() {
        return units;
    }

    /**
     * Returns the inner class loader.
     *
     * @return Class loader.
     */
    public ClassLoader classLoader() {
        if (impl == null) {
            synchronized (this) {
                if (impl == null) {
                    impl = createClassLoader();
                }
            }
        }
        return impl;
    }

    private JobClassLoaderImpl createClassLoader() {
        return AccessController.doPrivileged((PrivilegedAction<JobClassLoaderImpl>) () -> {
            URL[] classpath = units.stream()
                    .map(DisposableDeploymentUnit::path)
                    .flatMap(JobClasspath::collectClasspath)
                    .toArray(URL[]::new);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Created class loader with classpath: {}", Arrays.toString(classpath));
            }

            return new JobClassLoaderImpl(units, classpath, parent);
        });
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
            JobClassLoaderImpl impl0 = impl;

            if (impl0 != null) {
                impl0.close();
            }
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
