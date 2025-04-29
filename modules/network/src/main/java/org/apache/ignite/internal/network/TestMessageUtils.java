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

package org.apache.ignite.internal.network;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Pamparam.
 */
public class TestMessageUtils {
    private static final Class<?> testMessageClass;
    private static final Method msgMethod;
    private static final Method mapMethod;

    private static long nanotimeBase = System.nanoTime();

    static {
        Class<?> clazz;
        try {
            clazz = Class.forName("org.apache.ignite.internal.network.messages.TestMessage");
        } catch (ClassNotFoundException e) {
            clazz = null;
        }
        testMessageClass = clazz;

        if (clazz != null) {
            Method msgMethod0;
            try {
                msgMethod0 = clazz.getMethod("msg");
            } catch (NoSuchMethodException e) {
                msgMethod0 = null;
            }
            msgMethod = msgMethod0;

            Method mapMethod0;
            try {
                mapMethod0 = clazz.getMethod("map");
            } catch (NoSuchMethodException e) {
                mapMethod0 = null;
            }
            mapMethod = mapMethod0;
        } else {
            msgMethod = null;
            mapMethod = null;
        }
    }

    /**
     * Pum.
     */
    public static void addThreadInfo(int key, NetworkMessage message) {
        if (testMessageClass != null) {
            if (testMessageClass.isInstance(message)) {
                Map<Integer, String> map = getMap(message);

                if (map != null) {
                    map.put(key, getThreadInfo());
                }
            }
        }
    }

    private static String getThreadInfo() {
        Thread currentThread = Thread.currentThread();
        return currentThread.getName() + "/" + currentThread.getId();
    }

    private static String getMessage(NetworkMessage message) {
        try {
            return (String) msgMethod.invoke(message);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<Integer, String> getMap(NetworkMessage message) {
        try {
            return (Map<Integer, String>) mapMethod.invoke(message);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Purum.
     */
    public static void extendHistory(NetworkMessage message, String description) {
        extendHistory(message, false, description);
    }

    /**
     * Purum.
     */
    public static void extendHistory(NetworkMessage message, boolean writeTrace, String description) {
        if (testMessageClass != null) {
            if (testMessageClass.isInstance(message)) {
                Map<Integer, String> map = getMap(message);

                if (map != null) {
                    String suffix;
                    if (writeTrace && "second".equals(getMessage(message))) {
                        suffix = "\n" + ExceptionUtils.getFullStackTrace(new Exception("Tracking"));
                    } else {
                        suffix = "";
                    }

                    map.compute(555, (k, prev) -> {
                        HistoryItem historyItem = new HistoryItem(description);
                        return (prev == null ? historyItem.toString() : prev + "\n" + historyItem) + suffix;
                    });
                }
            }
        }
    }

    public static void rememberTime() {
        nanotimeBase = System.nanoTime();
    }

    public static @Nullable String getMessageIfTest(NetworkMessage message) {
        if (testMessageClass != null && testMessageClass.isInstance(message)) {
            return getMessage(message);
        } else {
            return null;
        }
    }

    public static class HistoryItem {
        private final String description;
        private final long timestampNanos;
        private final String threadInfo;

        public HistoryItem(String description) {
            this(description, System.nanoTime() - nanotimeBase, getThreadInfo());
        }

        private HistoryItem(String description, long timestampNanos, String threadInfo) {
            this.description = description;
            this.timestampNanos = timestampNanos;
            this.threadInfo = threadInfo;
        }

        @Override
        public String toString() {
            return "HistoryItem{" +
                    "description='" + description + '\'' +
                    ", timestampNanos=" + timestampNanos +
                    ", threadInfo='" + threadInfo + '\'' +
                    '}';
        }
    }
}
