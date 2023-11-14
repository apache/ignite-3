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

import static org.apache.ignite.lang.ErrorGroups.Compute.CLASS_INITIALIZATION_ERR;

import java.lang.reflect.Constructor;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteException;

/**
 * Utility class for compute.
 */
public class ComputeUtils {
    /**
     * Instantiate compute job via provided class loader by provided job class name.
     *
     * @param jobClassLoader Class loader.
     * @param jobClassName Job class name.
     * @param <R> Compute job return type.
     * @return Compute job instance.
     */
    public static <R> ComputeJob<R> instantiateJob(ClassLoader jobClassLoader, String jobClassName) {
        Class<ComputeJob<R>> computeJobClass = jobClass(jobClassLoader, jobClassName);
        if (!(ComputeJob.class.isAssignableFrom(computeJobClass))) {
            throw new IgniteException(
                    CLASS_INITIALIZATION_ERR,
                    "'" + computeJobClass.getName() + "' does not implement ComputeJob interface"
            );
        }

        try {
            Constructor<? extends ComputeJob<R>> constructor = computeJobClass.getDeclaredConstructor();

            if (!constructor.canAccess(null)) {
                constructor.setAccessible(true);
            }

            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IgniteInternalException(
                    CLASS_INITIALIZATION_ERR,
                    "Cannot instantiate job",
                    e
            );
        }
    }

    private static <R, J extends ComputeJob<R>> Class<J> jobClass(ClassLoader jobClassLoader, String jobClassName) {
        try {
            return (Class<J>) Class.forName(jobClassName, true, jobClassLoader);
        } catch (ClassNotFoundException e) {
            throw new IgniteInternalException(
                    CLASS_INITIALIZATION_ERR,
                    "Cannot load job class by name '" + jobClassName + "'",
                    e
            );
        }
    }
}
