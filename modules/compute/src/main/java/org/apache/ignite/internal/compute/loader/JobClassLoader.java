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
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.ignite.internal.deployunit.DisposableDeploymentUnit;
import org.apache.ignite.lang.ErrorGroups.Compute;
import org.apache.ignite.lang.IgniteException;

/**
 * Implementation of {@link ClassLoader} that loads classes from specified component directories.
 */
public class JobClassLoader extends URLClassLoader {
    /**
     * Pattern to match system packages.
     */
    private static final Pattern SYSTEM_PACKAGES_PATTERN = Pattern.compile("^(java|x|javax|org\\.apache\\.ignite)\\..*$");

    private final List<DisposableDeploymentUnit> units;

    /**
     * Parent class loader.
     */
    private final ClassLoader parent;

    /**
     * Creates new instance of {@link JobClassLoader}.
     *
     * @param urls URLs to load classes from.
     * @param units Units to load classes from.
     * @param parent Parent class loader.
     */
    public JobClassLoader(List<DisposableDeploymentUnit> units, URL[] urls, ClassLoader parent) {
        super("compute-job", urls, parent);
        this.units = units;
        this.parent = parent;
    }

    public List<DisposableDeploymentUnit> units() {
        return units;
    }

    /**
     * Loads the class with the specified <a href="#binary-name">binary name</a>. The implementation of this method searches for classes in
     * the following order:
     *
     * <ol>
     *
     *   <li><p> If the name starts with one of {@link JobClassLoader#SYSTEM_PACKAGES_PATTERN},
     *   the loader delegates search to the parent.  </p></li>
     *
     *   <li><p> If the name doesn't start with one of {@link JobClassLoader#SYSTEM_PACKAGES_PATTERN}:
     *
     *   <ol>
     *      <li><p> Invoke {@link #findLoadedClass(String)} to check if the class has already been loaded.  </p></li>
     *      <li><p> Invoke the {@link #findClass(String)} method to find the
     *      class.  </p></li>
     *      <li><p> Invoke the {@link #loadClass(String) loadClass} method
     *      on the parent class loader.  </p></li>
     *   </ol></p></li>
     *
     * </ol>
     *
     * @param name The <a href="#binary-name">binary name</a> of the class.
     * @return The resulting {@code Class} object.
     * @throws ClassNotFoundException If the class was not found.
     */
    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        boolean isSystem = SYSTEM_PACKAGES_PATTERN.matcher(name).find();
        if (isSystem) {
            try {
                return parent.loadClass(name);
            } catch (ClassNotFoundException exception) {
                return loadClassFromClasspath(name, resolve);
            }
        } else {
            try {
                return loadClassFromClasspath(name, resolve);
            } catch (ClassNotFoundException exception) {
                return parent.loadClass(name);
            }
        }
    }

    private Class<?> loadClassFromClasspath(String name, boolean resolve) throws ClassNotFoundException {
        Class<?> loadedClass = findLoadedClass(name);
        if (loadedClass == null) {
            Class<?> clazz = findClass(name);
            if (resolve) {
                resolveClass(clazz);
            }
            return clazz;
        } else {
            return loadedClass;
        }
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
            super.close();
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

    @Override
    public String toString() {
        return "JobClassLoader{"
                + "classpath=" + Arrays.toString(getURLs())
                + '}';
    }
}
