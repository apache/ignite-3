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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;

/**
 * Creates a class loader for a job.
 */
public class JobClassLoaderFactory {

    /**
     * Directory for components.
     */
    private final File componentsDir;

    /**
     * Constructor.
     *
     * @param componentsDir The directory for components.
     */
    public JobClassLoaderFactory(File componentsDir) {
        this.componentsDir = componentsDir;
    }

    /**
     * Create a class loader for the specified components.
     *
     * @param components The components of the job.
     * @return The class loader.
     */
    public ComponentClassLoader createClassLoader(List<String> components) {
        if (components.isEmpty()) {
            throw new IllegalArgumentException("No components specified");
        }

        URL[] classPath = components.stream()
                .map(it -> new File(componentsDir, it))
                .map(JobClassLoaderFactory::constructClasspath)
                .flatMap(Arrays::stream)
                .toArray(URL[]::new);

        return new ComponentClassLoader(classPath, getClass().getClassLoader());
    }

    private static URL[] constructClasspath(File componentDir) {
        if (!componentDir.exists()) {
            throw new IllegalArgumentException("Component does not exist: " + componentDir);
        }

        if (!componentDir.isDirectory()) {
            throw new IllegalArgumentException("Component is not a directory: " + componentDir);
        }

        // Construct the "class path" for this component
        return Arrays.stream(componentDir.listFiles())
                .map(it -> {
                    try {
                        return it.getCanonicalFile().toURI().toURL();
                    } catch (IOException e) {
                        throw new IgniteException(
                                Common.UNEXPECTED_ERR,
                                "Failed to add component to classpath: " + componentDir.getAbsolutePath(),
                                e
                        );
                    }
                })
                .toArray(URL[]::new);
    }
}
