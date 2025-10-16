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

package org.apache.ignite.internal.lang;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.apache.ignite.internal.components.NodeProperties;
import org.jetbrains.annotations.Nullable;

/**
 * Contains constants for all system properties and environmental variables in Ignite. These properties and variables can be used to affect
 * the behavior of Ignite.
 */
public final class IgniteSystemProperties {
    /** Name of the property controlling whether thread assertions are enabled. */
    public static final String THREAD_ASSERTIONS_ENABLED = "IGNITE_THREAD_ASSERTIONS_ENABLED";

    /** Name of the property controlling whether logging long handling of messages and events is enabled. */
    public static final String LONG_HANDLING_LOGGING_ENABLED = "IGNITE_LONG_HANDLING_LOGGING_ENABLED";

    /** Name of the property controlling whether, when a thread assertion is triggered, it should also be written to the log. */
    public static final String THREAD_ASSERTIONS_LOG_BEFORE_THROWING = "THREAD_ASSERTIONS_LOG_BEFORE_THROWING";

    /** Skip replication in a benchmark. */
    public static final String IGNITE_SKIP_REPLICATION_IN_BENCHMARK = "IGNITE_SKIP_REPLICATION_IN_BENCHMARK";

    /** Skip storage update in a benchmark. */
    public static final String IGNITE_SKIP_STORAGE_UPDATE_IN_BENCHMARK = "IGNITE_SKIP_STORAGE_UPDATE_IN_BENCHMARK";

    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove this feature flag.
    /**
     * Enables zone based replication (aka colocation) feature.
     *
     * @deprecated Non-colocation mode is planned to be removed in version 3.2.
     */
    @Deprecated(since = "3.1", forRemoval = true)
    public static final String COLOCATION_FEATURE_FLAG = "IGNITE_ZONE_BASED_REPLICATION";

    /**
     * Enforces singleton.
     */
    private IgniteSystemProperties() {
        // No-op.
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Remove.
    /**
     * Feature flag for zone based colocation track.
     *
     * <p>Do not use in production code (apart from {@link NodeProperties} implementations). If a component needs colocation status,
     * it should get one from {@link NodeProperties}.
     *
     * @deprecated Non-colocation mode is planned to be removed in version 3.2.
     */
    @Deprecated(since = "3.1", forRemoval = true)
    public static boolean colocationEnabled() {
        return getBoolean(COLOCATION_FEATURE_FLAG, true);
    }

    /**
     * Gets either system property or environment variable with given name and convert to enum of given class.
     *
     * @param enumCls Enum class.
     * @param name    Name of the system property or environment variable.
     * @param <E>     Enum type.
     * @return Enum value or {@code null} if the property is not set.
     */
    public static <E extends Enum<E>> E getEnum(Class<E> enumCls, String name) {
        return getEnum(enumCls, name, null);
    }

    /**
     * Gets either system property or environment variable with given name and convert to enum of given class.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @param <E>  Enum type.
     * @return Enum value or the given default.
     */
    public static <E extends Enum<E>> E getEnum(String name, E dflt) {
        return getEnum(dflt.getDeclaringClass(), name, dflt);
    }

    /**
     * Gets either system property or environment variable with given name and convert to enum of given class.
     *
     * @param enumCls Enum type.
     * @param name    Name of the system property or environment variable.
     * @param dflt    Default value.
     * @param <E>     Enum type.
     * @return Enum value or the given default.
     */
    private static <E extends Enum<E>> E getEnum(Class<E> enumCls, String name, E dflt) {
        assert enumCls != null;

        String val = getString(name);

        if (val == null) {
            return dflt;
        }

        try {
            return Enum.valueOf(enumCls, val);
        } catch (IllegalArgumentException ignore) {
            return dflt;
        }
    }

    /**
     * Gets either system property or environment variable with given name.
     *
     * @param name Name of the system property or environment variable.
     * @return Value of the system property or environment variable. Returns {@code null} if neither can be found for given name.
     */
    @Nullable
    public static String getString(String name) {
        assert name != null;

        String v = System.getProperty(name);

        if (v == null) {
            v = System.getenv(name);
        }

        return v;
    }

    /**
     * Gets either system property or environment variable with given name.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Value of the system property or environment variable. Returns {@code dflt} if neither can be found for given name.
     */
    public static String getString(String name, String dflt) {
        String val = getString(name);

        return val == null ? dflt : val;
    }

    /**
     * Gets either system property or environment variable with given name. The result is transformed to {@code boolean} using {@code
     * Boolean.valueOf()} method.
     *
     * @param name Name of the system property or environment variable.
     * @return Boolean value of the system property or environment variable. Returns {@code False} in case neither system property nor
     *      environment variable with given name is found.
     */
    public static boolean getBoolean(String name) {
        return getBoolean(name, false);
    }

    /**
     * Gets either system property or environment variable with given name. The result is transformed to {@code boolean} using {@code
     * Boolean.valueOf()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Boolean value of the system property or environment variable. Returns default value in case neither system property nor
     *      environment variable with given name is found.
     */
    public static boolean getBoolean(String name, boolean dflt) {
        String val = getString(name);

        return val == null ? dflt : Boolean.parseBoolean(val);
    }

    /**
     * Gets either system property or environment variable with given name. The result is transformed to {@code int} using {@code
     * Integer.parseInt()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Integer value of the system property or environment variable. Returns default value in case neither system property nor
     *      environment variable with given name is found.
     */
    public static int getInteger(String name, int dflt) {
        String s = getString(name);

        if (s == null) {
            return dflt;
        }

        int res;

        try {
            res = Integer.parseInt(s);
        } catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }

    /**
     * Gets either system property or environment variable with given name. The result is transformed to {@code float} using {@code
     * Float.parseFloat()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Float value of the system property or environment variable. Returns default value in case neither system property nor
     *      environment variable with given name is found.
     */
    public static float getFloat(String name, float dflt) {
        String s = getString(name);

        if (s == null) {
            return dflt;
        }

        float res;

        try {
            res = Float.parseFloat(s);
        } catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }

    /**
     * Gets either system property or environment variable with given name. The result is transformed to {@code long} using {@code
     * Long.parseLong()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Integer value of the system property or environment variable. Returns default value in case neither system property nor
     *      environment variable with given name is found.
     */
    public static long getLong(String name, long dflt) {
        String s = getString(name);

        if (s == null) {
            return dflt;
        }

        long res;

        try {
            res = Long.parseLong(s);
        } catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }

    /**
     * Gets either system property or environment variable with given name. The result is transformed to {@code double} using {@code
     * Double.parseDouble()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Integer value of the system property or environment variable. Returns default value in case neither system property nor
     *      environment variable with given name is found.
     */
    public static double getDouble(String name, double dflt) {
        String s = getString(name);

        if (s == null) {
            return dflt;
        }

        double res;

        try {
            res = Double.parseDouble(s);
        } catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }

    /**
     * Gets snapshot of system properties. Snapshot could be used for thread safe iteration over system properties. Non-string properties
     * are removed before return.
     *
     * @return Snapshot of system properties.
     */
    public static Properties snapshot() {
        Properties sysProps = (Properties) System.getProperties().clone();

        Iterator<Map.Entry<Object, Object>> iter = sysProps.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry entry = iter.next();

            if (!(entry.getValue() instanceof String) || !(entry.getKey() instanceof String)) {
                iter.remove();
            }
        }

        return sysProps;
    }
}
