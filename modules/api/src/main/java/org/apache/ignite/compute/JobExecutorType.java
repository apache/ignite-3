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

package org.apache.ignite.compute;

/**
 * Job executor type.
 */
public enum JobExecutorType {
    /**
     * Embedded Java job executor. Executes Java jobs in the same process and JVM where the Ignite node is running.
     */
    JAVA_EMBEDDED,

    /**
     * Side-car .NET job executor. Executes .NET jobs in a separate process.
     *
     * <p>Starts the process on demand. Requires .NET runtime to be installed.
     */
    DOTNET_SIDECAR;

    /** Cached array with all enum values. */
    private static final JobExecutorType[] VALUES = values();

    /**
     * Returns the enumerated value from its ordinal.
     *
     * @param ordinal Ordinal of enumeration constant.
     * @throws IllegalArgumentException If no enumeration constant by ordinal.
     */
    public static JobExecutorType fromOrdinal(int ordinal) throws IllegalArgumentException {
        if (ordinal < 0 || ordinal >= VALUES.length) {
            throw new IllegalArgumentException("No enum constant from ordinal: " + ordinal);
        }

        return VALUES[ordinal];
    }
}
