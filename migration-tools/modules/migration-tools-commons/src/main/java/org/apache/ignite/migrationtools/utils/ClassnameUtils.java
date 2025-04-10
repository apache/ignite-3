/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.utils;

import java.util.Map;

/** Small utilities for handling class names. */
public final class ClassnameUtils {

    public static final Map<String, String> PRIMITIVE_TO_WRAPPER = Map.of(
            boolean.class.getName(), Boolean.class.getName(),
            byte.class.getName(), Byte.class.getName(),
            char.class.getName(), Character.class.getName(),
            short.class.getName(), Short.class.getName(),
            int.class.getName(), Integer.class.getName(),
            long.class.getName(), Long.class.getName(),
            double.class.getName(), Double.class.getName(),
            float.class.getName(), Float.class.getName(),
            void.class.getName(), Void.class.getName()
    );

    private ClassnameUtils() {
        // Intentionally left blank
    }

    public static String ensureWrapper(String className) {
        return PRIMITIVE_TO_WRAPPER.getOrDefault(className, className);
    }
}
