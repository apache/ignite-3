package org.apache.ignite.internal.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ThreadFactory;

public class ThreadUtils {
    public static ThreadFactory virtualWorkerFactory(String prefix) {
        // This API is only available in JDK 21+. Use reflection to make the code compilable with lower JDKs.
        try {
            Method m = Class.forName("java.lang.Thread").getMethod("ofVirtual");
            Object threadBuilder = m.invoke(null);
            Class<?> threadBuilderClazz = Class.forName("java.lang.Thread$Builder");
            m = threadBuilderClazz.getMethod("name", String.class, long.class);
            m.invoke(threadBuilder, prefix + "-", 0L);
            m = threadBuilderClazz.getMethod("factory");
            return (ThreadFactory) m.invoke(threadBuilder);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException |
                 ClassNotFoundException | NullPointerException e) {
            throw new RuntimeException("Cannot instantiate VirtualThreadFactory", e);
        }
    }
}
