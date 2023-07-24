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

package org.apache.ignite.lang;

import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.privateLookupIn;
import static java.lang.invoke.MethodHandles.publicLookup;
import static java.lang.invoke.MethodType.methodType;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.invoke.WrongMethodTypeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class to wrap Ignite exceptions.
 */
public class IgniteExceptionUtils {
    /** Private constructor to prohibit creating an instance of utility class. */
    private IgniteExceptionUtils() {
        // No-op.
    }

    /**
     * Creates and returns a copy of an exception that is a cause of the given {@code CompletionException}.
     * If the original exception does not contain a cause, then the original exception will be returned.
     * In order to preserve a stack trace, the original completion exception will be set as the cause of the newly created exception.
     * <p>
     *     For example, this method might be useful when you need to implement sync API over async one.
     * </p>
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
     * <p>
     *     For example, this method might be useful when you need to implement sync API over async one.
     * </p>
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
    public static <T extends Throwable> T copyExceptionWithCause(
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

        e = ExceptionUtils.unwrapCause(e);

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
        EXCEPTION_FACTORIES = new ArrayList<>();

        // The most specific signatures should go in the first place.
        // Exception(UUID traceId, int code, String message, Throwable cause)
        EXCEPTION_FACTORIES.add(new ExceptionFactory(methodType(void.class, UUID.class, int.class, String.class, Throwable.class)) {
            @Override
            <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                    throws Throwable {
                return (T) constructor.invokeWithArguments(traceId, code, message, cause);
            }
        });
        // Exception(UUID traceId, int code, String message)
        EXCEPTION_FACTORIES.add(new ExceptionFactory(methodType(void.class, UUID.class, int.class, String.class)) {
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
        });
        // Exception(UUID traceId, int code, Throwable cause)
        EXCEPTION_FACTORIES.add(new ExceptionFactory(methodType(void.class, UUID.class, int.class, Throwable.class)) {
            @Override
            <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                    throws Throwable {
                if (cause != null) {
                    // Workaround to avoid error code duplication in exception message.
                    cause = new UtilException(message, cause);
                }
                return (T) constructor.invokeWithArguments(traceId, code, cause);
            }
        });
        // Exception(int code, String message, Throwable cause)
        EXCEPTION_FACTORIES.add(new ExceptionFactory(methodType(void.class, int.class, String.class, Throwable.class)) {
            @Override
            <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                    throws Throwable {
                return (T) constructor.invokeWithArguments(code, message, cause);
            }
        });
        // Exception(int code, String message)
        EXCEPTION_FACTORIES.add(new ExceptionFactory(methodType(void.class, int.class, String.class)) {
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
        });
        // Exception(int code, Throwable cause)
        EXCEPTION_FACTORIES.add(new ExceptionFactory(methodType(void.class, int.class, Throwable.class)) {
            @Override
            <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                    throws Throwable {
                if (cause != null) {
                    // Workaround to avoid error code duplication in exception message.
                    cause = new UtilException(message, cause);
                }
                return (T) constructor.invokeWithArguments(code, cause);
            }
        });
        // Exception(UUID traceId, int code)
        EXCEPTION_FACTORIES.add(new ExceptionFactory(methodType(void.class, UUID.class, int.class)) {
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
        });
        // Exception(String msg, Throwable cause)
        EXCEPTION_FACTORIES.add(new ExceptionFactory(methodType(void.class, String.class, Throwable.class)) {
            @Override
            <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                    throws Throwable {
                return (T) constructor.invokeWithArguments(message, cause);
            }
        });
        // Exception(int code)
        EXCEPTION_FACTORIES.add(new ExceptionFactory(methodType(void.class, int.class)) {
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
        });
        // Exception(String msg)
        EXCEPTION_FACTORIES.add(new ExceptionFactory(methodType(void.class, String.class)) {
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
        });
        // Exception(Throwable cause)
        EXCEPTION_FACTORIES.add(new ExceptionFactory(methodType(void.class, Throwable.class)) {
            @Override
            <T extends Throwable> T copy(MethodHandle constructor, UUID traceId, int code, String message, Throwable cause)
                    throws Throwable {
                if (cause != null) {
                    // Workaround to avoid error code duplication in exception message.
                    cause = new UtilException(message, cause);
                }
                return (T) constructor.invokeWithArguments(cause);
            }
        });
        // Exception()
        EXCEPTION_FACTORIES.add(new ExceptionFactory(methodType(void.class)) {
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
        });
    }

    /**
     * This class is used as workaround to avoid error code and trace id duplication in the error message.
     * The root cause of this issue is that the constructor Throwable(Throwable cause) uses cause.toString() method
     * to create a detailedMessage instead of getMessage(), ans so this message will be enriched by class name, error code and trace id.
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
     *     // but it should be just as follows: XYZ wrong argument
     *     copy.getMessage();
     *
     *     // The following line prints: IGN_SPECIFIC_ERR_CODE TraceId XYZ CustomException IGN_SPECIFIC_ERR_CODE TraceId XYZ wrong argument
     *     copy.toString();
     * </code></pre>
     *
     */
    private static class UtilException extends Throwable {
        public UtilException(String message, Throwable t) {
            super(message, t, false, false);
        }
    }
}
