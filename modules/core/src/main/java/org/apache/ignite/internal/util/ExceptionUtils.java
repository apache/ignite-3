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

package org.apache.ignite.internal.util;

import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.privateLookupIn;
import static java.lang.invoke.MethodHandles.publicLookup;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.newSetFromMap;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Transactions.TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.invoke.WrongMethodTypeException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteQuadFunction;
import org.apache.ignite.internal.lang.IgniteTriFunction;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.TraceableException;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Exception utils.
 */
public final class ExceptionUtils {
    /**
     * The names of methods commonly used to access a wrapped exception.
     */
    private static final String[] CAUSE_METHOD_NAMES = {
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
            causeMtd = Throwable.class.getMethod("getCause", (Class<?>) null);
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
    @Nullable
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
     * @param mtdName The name of the method to find and invoke.
     * @return The wrapped exception, or {@code null} if not found.
     */
    private static @Nullable Throwable getCauseUsingMethodName(Throwable throwable, String mtdName) {
        Method mtd = null;

        try {
            mtd = throwable.getClass().getMethod(mtdName, (Class<?>) null);
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
    private static @Nullable Throwable getCauseUsingFieldName(Throwable throwable, String fieldName) {
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
                Method mtd = cls.getMethod(methodName, (Class<?>) null);

                if (Throwable.class.isAssignableFrom(mtd.getReturnType())) {
                    return true;
                }
            } catch (NoSuchMethodException | SecurityException ignored) {
                // exception ignored
            }
        }

        try {
            cls.getField("detail");

            return true;
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
    public static @Nullable Throwable getCause(Throwable throwable) {
        return getCause(throwable, CAUSE_METHOD_NAMES);
    }

    /**
     * Introspects the {@code Throwable} to obtain the cause.
     *
     * @param throwable The throwable to introspect for a cause, may be null.
     * @param mtdNames  The method names, null treated as default set.
     * @return The cause of the {@code Throwable}, {@code null} if none found or null throwable input.
     */
    @Nullable
    public static Throwable getCause(@Nullable Throwable throwable, String[] mtdNames) {
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
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     * Note that this method follows includes {@link Throwable#getSuppressed()}
     * into check.
     *
     * @param throwable Throwable to check (if {@code null}, {@code false} is returned).
     * @param clazz Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code true} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    public static boolean hasCauseOrSuppressed(
            @Nullable Throwable throwable,
            Class<?> @Nullable ... clazz
    ) {
        return hasCauseOrSuppressed(throwable, null, clazz);
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     * Note that this method follows includes {@link Throwable#getSuppressed()}
     * into check.
     *
     * @param throwable Throwable to check (if {@code null}, {@code false} is returned).
     * @param message Error message fragment that should be in error message.
     * @param clazz Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code true} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    public static boolean hasCauseOrSuppressed(
            @Nullable Throwable throwable,
            @Nullable String message,
            Class<?> @Nullable ... clazz
    ) {
        return hasCauseOrSuppressedInternal(throwable, message, clazz, newSetFromMap(new IdentityHashMap<>()), true);
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     * Note that this method IGNORES {@link Throwable#getSuppressed()}.
     *
     * @param throwable Throwable to check (if {@code null}, {@code false} is returned).
     * @param clazz Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code true} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    public static boolean hasCause(
            @Nullable Throwable throwable,
            Class<?> @Nullable ... clazz
    ) {
        return hasCause(throwable, null, clazz);
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     * Note that this method IGNORES {@link Throwable#getSuppressed()}.
     *
     * @param throwable Throwable to check (if {@code null}, {@code false} is returned).
     * @param message Error message fragment that should be in error message.
     * @param clazz Cause classes to check (if {@code null} or empty, {@code false} is returned).
     * @return {@code true} if one of the causing exception is an instance of passed in classes,
     *      {@code false} otherwise.
     */
    public static boolean hasCause(
            @Nullable Throwable throwable,
            @Nullable String message,
            Class<?> @Nullable ... clazz
    ) {
        return hasCauseOrSuppressedInternal(throwable, message, clazz, newSetFromMap(new IdentityHashMap<>()), false);
    }

    /**
     * Internal method that does what is described in {@link #hasCauseOrSuppressed(Throwable, String, Class[])}, but protects against an
     * infinite loop. It can also consider or ignore suppressed exceptions.
     */
    private static boolean hasCauseOrSuppressedInternal(
            @Nullable Throwable throwable,
            @Nullable String message,
            Class<?> @Nullable [] clazz,
            Set<Throwable> dejaVu,
            boolean considerSuppressed
    ) {
        if (throwable == null || clazz == null || clazz.length == 0) {
            return false;
        }

        for (Throwable th = throwable; th != null; th = th.getCause()) {
            if (!dejaVu.add(th)) {
                break;
            }

            for (Class<?> c : clazz) {
                if (c.isAssignableFrom(th.getClass())) {
                    if (message != null) {
                        if (th.getMessage() != null && th.getMessage().contains(message)) {
                            return true;
                        } else {
                            continue;
                        }
                    }

                    return true;
                }
            }

            if (considerSuppressed) {
                for (Throwable n : th.getSuppressed()) {
                    if (hasCauseOrSuppressedInternal(n, message, clazz, dejaVu, true)) {
                        return true;
                    }
                }
            }

            if (th.getCause() == th) {
                break;
            }
        }

        return false;
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
     * Unwraps the root cause of the given exception.
     *
     * @param e The exception to unwrap.
     * @return The root cause of the exception, or the exception itself if no cause is found.
     */
    public static Throwable unwrapRootCause(Throwable e) {
        Throwable th = e.getCause();

        if (th == null) {
            return e;
        }

        while (th != e) {
            Throwable t = th;
            th = t.getCause();

            if (th == t || th == null) {
                return t;
            }
        }

        return e;
    }

    /**
     * Unwraps the cause from {@link CompletionException} if the provided exception is an instance of it.
     *
     * @param t Given throwable.
     * @return Unwrapped throwable.
     */
    @Nullable
    public static Throwable unwrapCompletionThrowable(@Nullable Throwable t) {
        if (t instanceof CompletionException) {
            return t.getCause();
        } else {
            return t;
        }
    }

    /**
     * Creates a new exception, which type is defined by the provided {@code supplier}, with the specified {@code t} as a cause.
     * In the case when the provided cause {@code t} is an instance of {@link TraceableException},
     * the original trace identifier and full error code are preserved.
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
     * In the case when the provided cause {@code t} is an instance of {@link TraceableException},
     * the original trace identifier and full error code are preserved.
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
            IgniteQuadFunction<UUID, Integer, String, Throwable, T> supplier,
            int defaultCode,
            String message,
            Throwable t
    ) {
        return withCauseInternal((traceId, code, m, cause) -> supplier.apply(traceId, code, message, t), defaultCode, t);
    }

    /**
     * Creates a new exception, which type is defined by the provided {@code supplier}, with the specified {@code t} as a cause
     * and full error code {@code code}.
     * In the case when the provided cause {@code t} is an instance of {@link TraceableException},
     * the original trace identifier preserved.
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
     * In the case when the provided cause {@code t} is an instance of {@link TraceableException},
     * the original trace identifier preserved.
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
            IgniteQuadFunction<UUID, Integer, String, Throwable, T> supplier,
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
            IgniteQuadFunction<UUID, Integer, String, Throwable, T> supplier,
            int defaultCode,
            Throwable t
    ) {
        Throwable unwrapped = unwrapCause(t);

        if (unwrapped instanceof TraceableException) {
            TraceableException traceable = (TraceableException) unwrapped;

            return supplier.apply(traceable.traceId(), traceable.code(), unwrapped.getMessage(), t);
        }

        return supplier.apply(UUID.randomUUID(), defaultCode, t.getMessage(), t);
    }

    /**
     * Creates and returns a copy of an exception that is a cause of the given {@code CompletionException}.
     * If the original exception does not contain a cause, then the original exception will be returned.
     * In order to preserve a stack trace, the original completion exception will be set as the cause of the newly created exception.
     *
     * <p>For example, this method might be useful when you need to implement sync API over async one.
     * <pre><code>
     *     public CompletableFuture&lt;Result&gt; asyncMethod {...}
     *
     *     public Result syncMethod() {
     *         try {
     *             return asyncMethod.join();
     *         } catch (CompletionException e) {
     *             throw sneakyThrow(copyExceptionWithCause(e));
     *         }
     *     }
     *
     * </code></pre>
     *
     * @param exception Original completion exception.
     * @return Copy of an exception that is a cause of the given {@code CompletionException}.
     */
    public static Throwable copyExceptionWithCause(CompletionException exception) {
        return copyExceptionWithCauseInternal(exception);
    }

    /**
     * Creates and returns a copy of an exception that is a cause of the given {@code ExecutionException}.
     * If the original exception does not contain a cause, then the original exception will be returned.
     * In order to preserve a stack trace, the original completion exception will be set as the cause of the newly created exception.
     *
     * <p>For example, this method might be useful when you need to implement sync API over async one.
     * <pre><code>
     *     public CompletableFuture&lt;Result&gt; asyncMethod {...}
     *
     *     public Result syncMethod() {
     *         try {
     *             return asyncMethod.get();
     *         } catch (ExecutionException e) {
     *             throw sneakyThrow(copyExceptionWithCause(e));
     *         }
     *     }
     *
     * </code></pre>
     *
     * @param exception Original execution exception.
     * @return Copy of an exception that is a cause of the given {@code ExecutionException}.
     */
    public static Throwable copyExceptionWithCause(ExecutionException exception) {
        return copyExceptionWithCauseInternal(exception);
    }

    /**
     * Creates and returns a new exception of the given {@code clazz} and with the specified parameters.
     *
     * @param clazz Class of the required exception to be instantiated.
     * @param traceId Trace identifier.
     * @param code Error code.
     * @param message Error message.
     * @param cause Cause.
     * @return New exception of the given {@code clazz} and with the specified parameters.
     */
    public static <T extends Throwable> @Nullable T copyExceptionWithCause(
            Class<? extends Throwable> clazz,
            @Nullable UUID traceId,
            int code,
            @Nullable String message,
            @Nullable Throwable cause
    ) {
        T copy = null;
        for (int i = 0; i < EXCEPTION_FACTORIES.size() && copy == null; ++i) {
            copy = EXCEPTION_FACTORIES.get(i).createCopy(clazz, traceId, code, message, cause);
        }
        return copy;
    }

    /**
     * Throws the given exception {@code e}.
     * This method allows to throw any checked exception without defining it explicitly in the method signature.
     *
     * @param e Exception to be thrown.
     * @return Actually, this method does not return anything, it just throws the provided exception.
     * @throws E Exception type.
     */
    public static <E extends Throwable> E sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }

    /**
     * Returns trace identifier of the given exception if it is ignite exception, and {@code null} otherwise.
     *
     * @param t Original exception that is used to extract a trace identifier from it.
     * @return Trace identifier.
     */
    public static @Nullable UUID extractTraceIdFrom(Throwable t) {
        if (t instanceof TraceableException) {
            return ((TraceableException) t).traceId();
        }

        return null;
    }

    /**
     * Returns error code of the given exception if it is ignite exception, and {@link ErrorGroups.Common#INTERNAL_ERR} otherwise.
     *
     * @param t Original exception that is used to extract a trace identifier from it.
     * @return Trace identifier.
     */
    public static int extractCodeFrom(Throwable t) {
        if (t instanceof TraceableException) {
            return ((TraceableException) t).code();
        }

        return INTERNAL_ERR;
    }

    /**
     * Determine if a particular error matches any of passed error codes.
     *
     * @param t Unwrapped throwable.
     * @param code The code.
     * @param codes Other codes.
     * @return {@code True} if exception allows retry.
     */
    public static boolean matchAny(Throwable t, int code, int... codes) {
        int errCode = extractCodeFrom(t);

        if (code == errCode) {
            return true;
        }

        for (int c0 : codes) {
            if (c0 == errCode) {
                return true;
            }
        }

        return false;
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-19870
    // This method should be removed or re-worked and usages should be changed to IgniteExceptionMapperUtil.mapToPublicException.
    /**
     * Wraps an exception in an IgniteException, extracting trace identifier and error code when the specified exception or one of its
     * causes is an IgniteException itself.
     *
     * @param e Internal exception.
     * @return Public exception.
     */
    @Deprecated(forRemoval = true)
    public static IgniteException wrap(Throwable e) {
        Objects.requireNonNull(e);

        e = unwrapCause(e);

        if (e instanceof IgniteException) {
            IgniteException iex = (IgniteException) e;

            try {
                return copyExceptionWithCause(e.getClass(), iex.traceId(), iex.code(), e.getMessage(), e);
            } catch (Exception ex) {
                throw new RuntimeException("IgniteException-derived class does not have required constructor: "
                        + e.getClass().getName(), ex);
            }
        }

        if (e instanceof IgniteCheckedException) {
            IgniteCheckedException iex = (IgniteCheckedException) e;

            return new IgniteException(iex.traceId(), iex.code(), e.getMessage(), e);
        }

        return new IgniteException(INTERNAL_ERR, e.getMessage(), e);
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy <b>including</b> that throwable itself.
     *
     * @param exceptionClass Cause class to check.
     * @param ex Throwable to check (if {@code null}, {@code false} is returned).
     * @return {@code True} if one of the causing exception is an instance of passed in classes, {@code false} otherwise.
     */
    public static boolean isOrCausedBy(Class<? extends Exception> exceptionClass, @Nullable Throwable ex) {
        return ex != null && (exceptionClass.isInstance(ex) || isOrCausedBy(exceptionClass, ex.getCause()));
    }

    /**
     * Returns {@code true} if the given throwable (or its cause) is a {@link TransactionException}
     * with {@link org.apache.ignite.lang.ErrorGroups.Transactions#TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR}.
     *
     * @param e Throwable to inspect.
     * @return {@code true} when the transaction was finished due to timeout, {@code false} otherwise.
     */
    public static boolean isFinishedDueToTimeout(Throwable e) {
        Throwable unwrapped = unwrapCause(e);
        if (!(unwrapped instanceof TransactionException)) {
            return false;
        }

        TransactionException ex = (TransactionException) unwrapped;

        return ex.code() == TX_ALREADY_FINISHED_WITH_TIMEOUT_ERR;
    }

    /**
     * Creates and return a copy of an exception that is a cause of the given {@code exception}.
     * If the original exception does not contain a cause, then the original exception will be returned.
     * In order to preserve a stack trace, the original completion exception will be set as the cause of the newly created exception.
     *
     * @param exception Original exception.
     * @return Copy of an exception that is a cause of the given {@code exception}.
     */
    private static <T extends Throwable> Throwable copyExceptionWithCauseInternal(T exception) {
        Throwable cause = exception.getCause();

        if (cause == null) {
            return exception;
        }

        UUID traceId = extractTraceIdFrom(cause);
        int code = extractCodeFrom(cause);

        return copyExceptionWithCause(cause.getClass(), traceId, code, cause.getMessage(), exception);
    }

    /**
     * Returns base Ignite exception class for the given {@code exception}.
     * The returned class can be one of the following: IgniteException, IgniteCheckedException, IgniteInternalException,
     * IgniteInternalCheckedException.
     * If the given {@code t} does not inherits any of these Ignite classes, then Throwable.class is returned.
     *
     * @param t Exception to be used in order to determine a base Ignite exception class.
     * @param <T> Exception type.
     * @return Returns base Ignite exception.
     */
    private static <T extends Throwable> Class<? extends Throwable> baseIgniteExceptionClass(T t) {
        if (t instanceof IgniteException) {
            return IgniteException.class;
        } else if (t instanceof IgniteCheckedException) {
            return IgniteCheckedException.class;
        } else if (t instanceof IgniteInternalException) {
            return IgniteInternalException.class;
        } else if (t instanceof IgniteInternalCheckedException) {
            return IgniteInternalCheckedException.class;
        }

        return Throwable.class;
    }

    /**
     * Base class for all factories that responsible for creating a new instance of an exception based on the defined signature.
     */
    private abstract static class ExceptionFactory {
        /** Constructor's signature. */
        private final MethodType signature;

        /**
         * Creates a new instance of a factory with the given signature.
         *
         * @param signature Constructor's signature.
         */
        ExceptionFactory(MethodType signature) {
            this.signature = signature;
        }

        /**
         * This method tries to create a new instance of exception based on the provided class and parameters.
         *
         * @param clazz Class of an exception that is required to construct.
         * @param traceId Trace identifier.
         * @param code Full error code.
         * @param message Detailed error message.
         * @param cause Cause.
         * @param <T> Type of returned exception.
         * @return a new instance of exception.
         *      Returned value can be {@code null} if the exception class cannot be constructed using a specific signature.
         */
        final <T extends Throwable> @Nullable T createCopy(
                Class<? extends Throwable> clazz,
                @Nullable UUID traceId,
                int code,
                @Nullable String message,
                @Nullable Throwable cause
        ) {
            try {
                MethodHandle constructor = publicLookup().findConstructor(clazz, signature);

                T exc = copy(constructor, traceId, code, message, cause);

                // Make sure that error code and trace id are the same.
                if (exc instanceof TraceableException) {
                    // Error code should be the same
                    assert code == extractCodeFrom(exc) :
                            "Unexpected error code [originCode=" + code + ", code=" + extractCodeFrom(exc) + ", err=" + exc + ']';

                    // Trace identifier can be generated internally by the exception class itself and should be overridden in that case.
                    try {
                        Class<? extends Throwable> baseExceptionClass = baseIgniteExceptionClass(exc);
                        privateLookupIn(baseExceptionClass, lookup())
                                .findSetter(baseExceptionClass, "traceId", UUID.class)
                                .invoke(exc, traceId);
                    } catch (Throwable ignore) {
                        // No-op.
                    }
                }

                return exc;
            } catch (NoSuchMethodException | IllegalAccessException | SecurityException | ClassCastException
                     | WrongMethodTypeException ignore) {
                // NoSuchMethodException, IllegalAccessException, SecurityException means that the required signature is not available.
                // ClassCastException - argument cannot be converted by reference casting.
                // WrongMethodTypeException - target's type cannot be adjusted to take the given parameters.
                // Just ignore and return a null value.
                return null;
            } catch (Throwable t) {
                // Exception has been thrown by the constructor itself.
                throw new IgniteException(INTERNAL_ERR, "Failed to create a copy of the required exception.", t);
            }
        }

        /**
         * Creates and returns a new instance using the given constructor.
         *
         * @param constructor Method to be invoked to instantiate a new exception.
         * @param traceId Trace identifier.
         * @param code Full error code.
         * @param message Detailed error message.
         * @param cause Cause.
         * @param <T> Type of returned exception.
         *
         * @return a new instance of exception. Returned value can be {@code null} if the exception class cannot be constructed
         *          using a specific signature.
         */
        @Nullable
        abstract <T extends Throwable> T copy(
                MethodHandle constructor,
                @Nullable UUID traceId,
                int code,
                @Nullable String message,
                @Nullable Throwable cause) throws Throwable;
    }

    /** Ordered collection of all possible factories. */
    private static final List<ExceptionFactory> EXCEPTION_FACTORIES;

    static {
        EXCEPTION_FACTORIES = List.of(
                // The most specific signatures should go in the first place.
                // Exception(UUID traceId, int code, String message, Throwable cause)
                new ExceptionFactory(methodType(void.class, UUID.class, int.class, String.class, Throwable.class)) {
                    @Override
                    <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                            throws Throwable {
                        return (T) constructor.invokeWithArguments(traceId, code, message, cause);
                    }
                },
                // Exception(UUID traceId, int code, String message)
                new ExceptionFactory(methodType(void.class, UUID.class, int.class, String.class)) {
                    @Override
                    <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                            throws Throwable {
                        T copy = (T) constructor.invokeWithArguments(traceId, code, message);
                        if (cause != null) {
                            try {
                                copy.initCause(cause);
                            } catch (IllegalStateException ignore) {
                                // No-op.
                            }
                        }
                        return copy;
                    }
                },
                // Exception(UUID traceId, int code, Throwable cause)
                new ExceptionFactory(methodType(void.class, UUID.class, int.class, Throwable.class)) {
                    @Override
                    <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                            throws Throwable {
                        if (cause != null) {
                            // Workaround to avoid error code duplication in exception message.
                            cause = new UtilException(message, cause);
                        }
                        return (T) constructor.invokeWithArguments(traceId, code, cause);
                    }
                },
                // Exception(int code, String message, Throwable cause)
                new ExceptionFactory(methodType(void.class, int.class, String.class, Throwable.class)) {
                    @Override
                    <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                            throws Throwable {
                        return (T) constructor.invokeWithArguments(code, message, cause);
                    }
                },
                // Exception(int code, String message)
                new ExceptionFactory(methodType(void.class, int.class, String.class)) {
                    @Override
                    <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                            throws Throwable {
                        T copy = (T) constructor.invokeWithArguments(code, message);
                        if (cause != null) {
                            try {
                                copy.initCause(cause);
                            } catch (IllegalStateException ignore) {
                                // No-op.
                            }
                        }
                        return copy;
                    }
                },
                // Exception(int code, Throwable cause)
                new ExceptionFactory(methodType(void.class, int.class, Throwable.class)) {
                    @Override
                    <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                            throws Throwable {
                        if (cause != null) {
                            // Workaround to avoid error code duplication in exception message.
                            cause = new UtilException(message, cause);
                        }
                        return (T) constructor.invokeWithArguments(code, cause);
                    }
                },
                // Exception(UUID traceId, int code)
                new ExceptionFactory(methodType(void.class, UUID.class, int.class)) {
                    @Override
                    <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                            throws Throwable {
                        T copy = (T) constructor.invokeWithArguments(traceId, code);
                        if (cause != null) {
                            try {
                                copy.initCause(cause);
                            } catch (IllegalStateException ignore) {
                                // No-op.
                            }
                        }
                        return copy;
                    }
                },
                // Exception(String msg, Throwable cause)
                new ExceptionFactory(methodType(void.class, String.class, Throwable.class)) {
                    @Override
                    <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                            throws Throwable {
                        return (T) constructor.invokeWithArguments(message, cause);
                    }
                },
                // Exception(int code)
                new ExceptionFactory(methodType(void.class, int.class)) {
                    @Override
                    <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                            throws Throwable {
                        T copy = (T) constructor.invokeWithArguments(code);
                        if (cause != null) {
                            try {
                                copy.initCause(cause);
                            } catch (IllegalStateException ignore) {
                                // No-op.
                            }
                        }
                        return copy;
                    }
                },
                // Exception(Throwable cause)
                new ExceptionFactory(methodType(void.class, Throwable.class)) {
                    @Override
                    <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                            throws Throwable {
                        if (cause != null) {
                            // Workaround to avoid error code duplication in exception message.
                            cause = new UtilException(message, cause);
                        }
                        return (T) constructor.invokeWithArguments(cause);
                    }
                },
                // Exception(String msg)
                new ExceptionFactory(methodType(void.class, String.class)) {
                    @Override
                    <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                            throws Throwable {
                        T copy = (T) constructor.invokeWithArguments(message);
                        if (cause != null) {
                            try {
                                copy.initCause(cause);
                            } catch (IllegalStateException ignore) {
                                // No-op.
                            }
                        }
                        return copy;
                    }
                },
                // Exception()
                new ExceptionFactory(methodType(void.class)) {
                    @Override
                    <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                            throws Throwable {
                        T copy = (T) constructor.invokeWithArguments();
                        if (cause != null) {
                            try {
                                copy.initCause(cause);
                            } catch (IllegalStateException ignore) {
                                // No-op.
                            }
                        }
                        return copy;
                    }
                }
        );
    }

    /**
     * This class is used as workaround to avoid error code and trace id duplication in the error message.
     * The root cause of this issue is that the constructor Throwable(Throwable cause) uses cause.toString() method
     * to create a detailedMessage instead of getMessage(), and so this message will be enriched by class name, error code and trace id.
     * For example,
     * <pre><code>
     *     class CustomException extends IgniteException {
     *         public CustomException(Throwable cause) {
     *             super(SPECIFIC_ERR_CODE, cause);
     *         }
     *     }
     *
     *     CustomException err = new CustomException(new IllegalArgumentException("wrong argument));
     *
     *     // The following line prints: wrong argument - valid.
     *     err.getMessage();
     *
     *     // The following line prints: IGN_SPECIFIC_ERR_CODE TraceId XYZ wrong argument - valid.
     *     err.toString();
     *
     *     CompletionException c = new CompletionException(err);
     *
     *     // The following line prints: IGN_SPECIFIC_ERR_CODE TraceId XYZ wrong argument
     *     c.getMessage();
     *
     *     CustomException copy = createCopyWithCause(c);
     *
     *     // The following line prints: CustomException IGN_SPECIFIC_ERR_CODE TraceId XYZ wrong argument
     *     // but it should be just as follows: wrong argument
     *     copy.getMessage();
     *
     *     // The following line prints: IGN_SPECIFIC_ERR_CODE TraceId XYZ CustomException IGN_SPECIFIC_ERR_CODE TraceId XYZ wrong argument
     *     copy.toString();
     * </code></pre>
     *
     */
    private static class UtilException extends RuntimeException {
        public UtilException(String message, Throwable t) {
            super(message, t, false, false);
        }
    }
}
