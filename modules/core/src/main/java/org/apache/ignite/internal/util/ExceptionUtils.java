/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteTetraFunction;
import org.apache.ignite.lang.IgniteTriFunction;
import org.jetbrains.annotations.Nullable;

/**
 * Exception utils.
 */
public final class ExceptionUtils {
    /**
     * The names of methods commonly used to access a wrapped exception.
     */
    private static final String[] CAUSE_METHOD_NAMES = new String[]{
            "getCause",
            "getNextException",
            "getTargetException",
            "getException",
            "getSourceException",
            "getRootCause",
            "getCausedByException",
            "getNested",
            "getLinkedException",
            "getNestedException",
            "getLinkedCause",
            "getThrowable"
    };

    /**
     * The Method object for Java 1.4 getCause.
     */
    private static final Method THROWABLE_CAUSE_METHOD;

    static {
        Method causeMtd;

        try {
            causeMtd = Throwable.class.getMethod("getCause", null);
        } catch (Exception ignored) {
            causeMtd = null;
        }

        THROWABLE_CAUSE_METHOD = causeMtd;
    }

    /**
     * Introspects the {@code Throwable} to obtain the cause.
     *
     * @param throwable The exception to examine.
     * @return The wrapped exception, or {@code null} if not found.
     */
    private static Throwable getCauseUsingWellKnownTypes(Throwable throwable) {
        if (throwable instanceof SQLException) {
            return ((SQLException) throwable).getNextException();
        }

        if (throwable instanceof InvocationTargetException) {
            return ((InvocationTargetException) throwable).getTargetException();
        }

        return null;
    }

    /**
     * Finds a {@code Throwable} by method name.
     *
     * @param throwable The exception to examine.
     * @param mtdName   The name of the method to find and invoke.
     * @return The wrapped exception, or {@code null} if not found.
     */
    private static Throwable getCauseUsingMethodName(Throwable throwable, String mtdName) {
        Method mtd = null;

        try {
            mtd = throwable.getClass().getMethod(mtdName, null);
        } catch (NoSuchMethodException | SecurityException ignored) {
            // exception ignored
        }

        if (mtd != null && Throwable.class.isAssignableFrom(mtd.getReturnType())) {
            try {
                return (Throwable) mtd.invoke(throwable, ArrayUtils.OBJECT_EMPTY_ARRAY);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ignored) {
                // exception ignored
            }
        }

        return null;
    }

    /**
     * Finds a {@code Throwable} by field name.
     *
     * @param throwable The exception to examine.
     * @param fieldName The name of the attribute to examine.
     * @return The wrapped exception, or {@code null} if not found.
     */
    private static Throwable getCauseUsingFieldName(Throwable throwable, String fieldName) {
        Field field = null;

        try {
            field = throwable.getClass().getField(fieldName);
        } catch (NoSuchFieldException | SecurityException ignored) {
            // exception ignored
        }

        if (field != null && Throwable.class.isAssignableFrom(field.getType())) {
            try {
                return (Throwable) field.get(throwable);
            } catch (IllegalAccessException | IllegalArgumentException ignored) {
                // exception ignored
            }
        }

        return null;
    }

    /**
     * Checks if the Throwable class has a {@code getCause} method.
     *
     * @return True if Throwable is nestable.
     */
    public static boolean isThrowableNested() {
        return THROWABLE_CAUSE_METHOD != null;
    }

    /**
     * Checks whether this {@code Throwable} class can store a cause.
     *
     * @param throwable The {@code Throwable} to examine, may be null.
     * @return Boolean {@code true} if nested otherwise {@code false}.
     */
    public static boolean isNestedThrowable(Throwable throwable) {
        if (throwable == null) {
            return false;
        }

        if (throwable instanceof SQLException || throwable instanceof InvocationTargetException) {
            return true;
        }

        if (isThrowableNested()) {
            return true;
        }

        Class<?> cls = throwable.getClass();
        for (String methodName : CAUSE_METHOD_NAMES) {
            try {
                Method mtd = cls.getMethod(methodName, null);

                if (mtd != null && Throwable.class.isAssignableFrom(mtd.getReturnType())) {
                    return true;
                }
            } catch (NoSuchMethodException | SecurityException ignored) {
                // exception ignored
            }
        }

        try {
            Field field = cls.getField("detail");

            if (field != null) {
                return true;
            }
        } catch (NoSuchFieldException | SecurityException ignored) {
            // exception ignored
        }

        return false;
    }

    /**
     * Introspects the {@code Throwable} to obtain the cause.
     *
     * @param throwable The throwable to introspect for a cause, may be null.
     * @return The cause of the {@code Throwable}, {@code null} if none found or null throwable input.
     */
    public static Throwable getCause(Throwable throwable) {
        return getCause(throwable, CAUSE_METHOD_NAMES);
    }

    /**
     * Introspects the {@code Throwable} to obtain the cause.
     *
     * @param throwable The throwable to introspect for a cause, may be null.
     * @param mtdNames  The method names, null treated as default set.
     * @return The cause of the {@code Throwable}, {@code null} if none found or null throwable input.
     */
    public static Throwable getCause(Throwable throwable, String[] mtdNames) {
        if (throwable == null) {
            return null;
        }

        Throwable cause = getCauseUsingWellKnownTypes(throwable);

        if (cause == null) {
            if (mtdNames == null) {
                mtdNames = CAUSE_METHOD_NAMES;
            }

            for (String mtdName : mtdNames) {
                if (mtdName != null) {
                    cause = getCauseUsingMethodName(throwable, mtdName);

                    if (cause != null) {
                        break;
                    }
                }
            }

            if (cause == null) {
                cause = getCauseUsingFieldName(throwable, "detail");
            }
        }

        return cause;
    }

    /**
     * Returns the list of {@code Throwable} objects in the exception chain.
     *
     * <p>A throwable without cause will return a list containing one element - the input throwable. A throwable with one cause
     * will return a list containing two elements - the input throwable and the cause throwable.
     * A {@code null} throwable will return a list of size zero.
     *
     * <p>This method handles recursive cause structures that might otherwise cause infinite loops. The cause chain is processed until
     * the end is reached, or until the next item in the chain is already in the result set.
     *
     * @param throwable The throwable to inspect, may be null.
     * @return The list of throwables, never null.
     */
    public static List<Throwable> getThrowableList(Throwable throwable) {
        List<Throwable> list = new ArrayList<>();

        while (throwable != null && !list.contains(throwable)) {
            list.add(throwable);
            throwable = getCause(throwable);
        }

        return list;
    }

    /**
     * Collects suppressed exceptions from throwable and all it causes.
     *
     * @param t Throwable.
     * @return List of suppressed throwables.
     */
    public static List<Throwable> getSuppressedList(@Nullable Throwable t) {
        List<Throwable> result = new ArrayList<>();

        if (t == null) {
            return result;
        }

        do {
            for (Throwable suppressed : t.getSuppressed()) {
                result.add(suppressed);

                result.addAll(getSuppressedList(suppressed));
            }
        } while ((t = t.getCause()) != null);

        return result;
    }

    /**
     * A way to get the entire nested stack-trace of an throwable.
     *
     * @param throwable The {@code Throwable} to be examined.
     * @return The nested stack trace, with the root cause first.
     */
    public static String getFullStackTrace(Throwable throwable) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);
        var ts = getThrowableList(throwable);

        for (Throwable t : ts) {
            t.printStackTrace(pw);

            if (isNestedThrowable(t)) {
                break;
            }
        }

        return sw.getBuffer().toString();
    }

    /**
     * Unwraps exception cause from wrappers like CompletionException and ExecutionException.
     *
     * @param e Throwable.
     * @return Unwrapped throwable.
     */
    public static Throwable unwrapCause(Throwable e) {
        while ((e instanceof CompletionException || e instanceof ExecutionException) && e.getCause() != null) {
            e = e.getCause();
        }

        return e;
    }

    /**
     * Creates a new exception, which type is defined by the provided {@code supplier}, with the specified {@code t} as a cause.
     * In the case when the provided cause {@code t} is an instance of {@link IgniteInternalException}
     * or {@link IgniteInternalCheckedException}, the original trace identifier and full error code are preserved.
     * Otherwise, a newly generated trace identifier and {@code defaultCode} are used.
     *
     * @param supplier Reference to a exception constructor.
     * @param defaultCode Error code to be used in the case when the provided cause {@code t} is not an instance of Ignite exception.
     * @param t Cause to be used.
     * @param <T> Type of a new exception.
     * @return New exception with the given cause.
     */
    public static <T extends Exception> T withCause(IgniteTriFunction<UUID, Integer, Throwable, T> supplier, int defaultCode, Throwable t) {
        return withCauseInternal((traceId, code, message, cause) -> supplier.apply(traceId, code, t), defaultCode, t);
    }

    /**
     * Creates a new exception, which type is defined by the provided {@code supplier}, with the specified {@code t} as a cause.
     * In the case when the provided cause {@code t} is an instance of {@link IgniteInternalException}
     * or {@link IgniteInternalCheckedException}, the original trace identifier and full error code are preserved.
     * Otherwise, a newly generated trace identifier and {@code defaultCode} are used.
     *
     * @param supplier Reference to a exception constructor.
     * @param defaultCode Error code to be used in the case when the provided cause {@code t} is not an instance of Ignite exception.
     * @param message Detailed error message.
     * @param t Cause to be used.
     * @param <T> Type of a new exception.
     * @return New exception with the given cause.
     */
    public static <T extends Exception> T withCause(
            IgniteTetraFunction<UUID, Integer, String, Throwable, T> supplier,
            int defaultCode,
            String message,
            Throwable t
    ) {
        return withCauseInternal((traceId, code, m, cause) -> supplier.apply(traceId, code, message, t), defaultCode, t);
    }

    /**
     * Creates a new exception, which type is defined by the provided {@code supplier}, with the specified {@code t} as a cause
     * and full error code {@code code}.
     * In the case when the provided cause {@code t} is an instance of {@link IgniteInternalException}
     * or {@link IgniteInternalCheckedException}, the original trace identifier preserved.
     * Otherwise, a newly generated trace identifier is used.
     *
     * @param supplier Reference to a exception constructor.
     * @param code New error code.
     * @param t Cause to be used.
     * @param <T> Type of a new exception.
     * @return New exception with the given cause.
     */
    public static <T extends Exception> T withCauseAndCode(IgniteTriFunction<UUID, Integer, Throwable, T> supplier, int code, Throwable t) {
        return withCauseInternal((traceId, c, message, cause) -> supplier.apply(traceId, code, t), code, t);
    }

    /**
     * Creates a new exception, which type is defined by the provided {@code supplier}, with the specified {@code t} as a cause,
     * full error code {@code code} and error message {@code message}.
     * In the case when the provided cause {@code t} is an instance of {@link IgniteInternalException}
     * or {@link IgniteInternalCheckedException}, the original trace identifier preserved.
     * Otherwise, a newly generated trace identifier is used.
     *
     * @param supplier Reference to a exception constructor.
     * @param code New error code.
     * @param message Detailed error message.
     * @param t Cause to be used.
     * @param <T> Type of a new exception.
     * @return New exception with the given cause.
     */
    public static <T extends Exception> T withCauseAndCode(
            IgniteTetraFunction<UUID, Integer, String, Throwable, T> supplier,
            int code,
            String message,
            Throwable t
    ) {
        return withCauseInternal((traceId, c, m, cause) -> supplier.apply(traceId, code, message, t), code, t);
    }

    /**
     * Extracts the trace identifier and full error code from ignite exception and creates a new one based on the provided {@code supplier}.
     *
     * @param supplier Supplier to create a concrete exception instance.
     * @param defaultCode Error code to be used in the case when the provided cause {@code t} is not an instance of Ignite exception.
     * @param t Cause.
     * @param <T> Type of a new exception.
     * @return New
     */
    private static <T extends Exception> T withCauseInternal(
            IgniteTetraFunction<UUID, Integer, String, Throwable, T> supplier,
            int defaultCode,
            Throwable t
    ) {
        Throwable unwrapped = unwrapCause(t);

        if (unwrapped instanceof IgniteInternalException) {
            IgniteInternalException iie = (IgniteInternalException) unwrapped;
            return supplier.apply(iie.traceId(), iie.code(), iie.getMessage(), t);
        } else if (unwrapped instanceof IgniteInternalCheckedException) {
            IgniteInternalCheckedException iice = (IgniteInternalCheckedException) unwrapped;
            return supplier.apply(iice.traceId(), iice.code(), iice.getMessage(), t);
        }

        return supplier.apply(UUID.randomUUID(), defaultCode, t.getMessage(), t);
    }
}
