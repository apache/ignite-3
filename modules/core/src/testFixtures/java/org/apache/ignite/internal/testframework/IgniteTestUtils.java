/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.internal.testframework;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestInfo;

/**
 * Utility class for tests.
 */
public final class IgniteTestUtils {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteTestUtils.class);

    private static final int TIMEOUT_SEC = 5000;

    /**
     * Set object field value via reflection.
     *
     * @param obj       Object to set field value to.
     * @param fieldName Field name to set value for.
     * @param val       New field value.
     * @throws IgniteInternalException In case of error.
     */
    public static void setFieldValue(Object obj, String fieldName, Object val) throws IgniteInternalException {
        assert obj != null;
        assert fieldName != null;

        try {
            Class<?> cls = obj instanceof Class ? (Class) obj : obj.getClass();

            Field field = cls.getDeclaredField(fieldName);

            boolean isFinal = (field.getModifiers() & Modifier.FINAL) != 0;

            boolean isStatic = (field.getModifiers() & Modifier.STATIC) != 0;

            /*
             * http://java.sun.com/docs/books/jls/third_edition/html/memory.html#17.5.3
             * If a final field is initialized to a compile-time constant in the field declaration,
             *   changes to the final field may not be observed.
             */
            if (isFinal && isStatic) {
                throw new IgniteInternalException("Modification of static final field through reflection.");
            }

            if (!field.isAccessible()) {
                field.setAccessible(true);
            }

            field.set(obj, val);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteInternalException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }

    /**
     * Set object field value via reflection.
     *
     * @param obj       Object to set field value to.
     * @param cls       Class to get field from.
     * @param fieldName Field name to set value for.
     * @param val       New field value.
     * @throws IgniteInternalException In case of error.
     */
    public static void setFieldValue(Object obj, Class cls, String fieldName, Object val) throws IgniteInternalException {
        assert fieldName != null;

        try {
            Field field = cls.getDeclaredField(fieldName);

            if (!field.isAccessible()) {
                field.setAccessible(true);
            }

            boolean isFinal = (field.getModifiers() & Modifier.FINAL) != 0;

            boolean isStatic = (field.getModifiers() & Modifier.STATIC) != 0;

            /*
             * http://java.sun.com/docs/books/jls/third_edition/html/memory.html#17.5.3
             * If a final field is initialized to a compile-time constant in the field declaration,
             *   changes to the final field may not be observed.
             */
            if (isFinal && isStatic) {
                throw new IgniteInternalException("Modification of static final field through reflection.");
            }

            if (isFinal) {
                Field modifiersField = Field.class.getDeclaredField("modifiers");

                modifiersField.setAccessible(true);

                modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            }

            field.set(obj, val);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteInternalException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }

    /**
     * Returns field value.
     *
     * @param target        target object from which to get field value ({@code null} for static methods)
     * @param declaredClass class on which the field is declared
     * @param fieldName     name of the field
     * @return field value
     */
    public static Object getFieldValue(Object target, Class<?> declaredClass, String fieldName) {
        Field field;
        try {
            field = declaredClass.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new IgniteInternalException("Did not find a field", e);
        }

        if (!field.isAccessible()) {
            field.setAccessible(true);
        }

        try {
            return field.get(target);
        } catch (IllegalAccessException e) {
            throw new IgniteInternalException("Cannot get field value", e);
        }
    }

    /**
     * Get object field value via reflection.
     *
     * @param obj Object or class to get field value from.
     * @param fieldNames Field names to get value for: obj->field1->field2->...->fieldN.
     * @param <T> Expected field class.
     * @return Field value.
     * @throws IgniteInternalException In case of error.
     */
    public static <T> T getFieldValue(Object obj, String... fieldNames) {
        assert obj != null;
        assert fieldNames != null;
        assert fieldNames.length >= 1;

        try {
            for (String fieldName : fieldNames) {
                Class<?> cls = obj instanceof Class ? (Class) obj : obj.getClass();

                try {
                    obj = findField(cls, obj, fieldName);
                } catch (NoSuchFieldException e) {
                    // Resolve inner class, if not an inner field.
                    Class<?> innerCls = getInnerClass(cls, fieldName);

                    if (innerCls == null) {
                        throw new IgniteInternalException("Failed to get object field [obj=" + obj
                                + ", fieldNames=" + Arrays.toString(fieldNames) + ']', e);
                    }

                    obj = innerCls;
                }
            }

            return (T) obj;
        } catch (IllegalAccessException e) {
            throw new IgniteInternalException("Failed to get object field [obj=" + obj
                    + ", fieldNames=" + Arrays.toString(fieldNames) + ']', e);
        }
    }

    /**
     * Get object field value via reflection.
     *
     * @param cls Class for searching.
     * @param obj Target object.
     * @param fieldName Field name for search.
     * @return Field from object if it was found.
     */
    private static Object findField(
            Class<?> cls,
            Object obj,
            String fieldName
    ) throws NoSuchFieldException, IllegalAccessException {
        // Resolve inner field.
        Field field = cls.getDeclaredField(fieldName);

        boolean accessible = field.isAccessible();

        if (!accessible) {
            field.setAccessible(true);
        }

        return field.get(obj);
    }

    /**
     * Get inner class by its name from the enclosing class.
     *
     * @param parentCls Parent class to resolve inner class for.
     * @param innerClsName Name of the inner class.
     * @return Inner class.
     */
    @Nullable public static <T> Class<T> getInnerClass(Class<?> parentCls, String innerClsName) {
        for (Class<?> cls : parentCls.getDeclaredClasses()) {
            if (innerClsName.equals(cls.getSimpleName())) {
                return (Class<T>) cls;
            }
        }

        return null;
    }

    /**
     * Checks whether runnable throws exception, which is itself of a specified class, or has a cause of the specified class.
     *
     * @param run Runnable to check.
     * @param cls Expected exception class.
     * @return Thrown throwable.
     */
    public static Throwable assertThrowsWithCause(
            @NotNull RunnableX run,
            @NotNull Class<? extends Throwable> cls
    ) {
        return assertThrowsWithCause(run, cls, null);
    }

    /**
     * Checks whether runnable throws exception, which is itself of a specified class, or has a cause of the specified class.
     *
     * @param run Runnable to check.
     * @param cls Expected exception class.
     * @param msg Message text that should be in cause (if {@code null}, message won't be checked).
     * @return Thrown throwable.
     */
    public static Throwable assertThrowsWithCause(
            @NotNull RunnableX run,
            @NotNull Class<? extends Throwable> cls,
            @Nullable String msg
    ) {
        try {
            run.run();
        } catch (Throwable e) {
            if (!hasCause(e, cls, msg)) {
                fail("Exception is neither of a specified class, nor has a cause of the specified class: " + cls, e);
            }

            return e;
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     *
     * <p>Note that this method follows includes {@link Throwable#getSuppressed()} into check.
     *
     * @param t   Throwable to check.
     * @param cls Cause classes to check.
     * @param msg Message text that should be in cause (if {@code null}, message won't be checked).
     * @return {@code True} if one of the causing exception is an instance of passed in classes, {@code false} otherwise.
     */
    public static boolean hasCause(
            @NotNull Throwable t,
            @NotNull Class<?> cls,
            @Nullable String msg
    ) {
        for (Throwable th = t; th != null; th = th.getCause()) {
            if (cls.isAssignableFrom(th.getClass())) {
                if (msg != null) {
                    if (th.getMessage() != null && th.getMessage().contains(msg)) {
                        return true;
                    } else {
                        continue;
                    }
                }

                return true;
            }

            for (Throwable n : th.getSuppressed()) {
                if (hasCause(n, cls, msg)) {
                    return true;
                }
            }

            if (th.getCause() == th) {
                break;
            }
        }

        return false;
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     *
     * <p>Note that this method follows includes {@link Throwable#getSuppressed()} into check.
     *
     * @param t   Throwable to check.
     * @param cls Cause classes to check.
     * @return reference to the cause error if found, otherwise returns {@code null}.
     */
    public static <T extends Throwable> T cause(
            @NotNull Throwable t,
            @NotNull Class<T> cls
    ) {
        return cause(t, cls, null);
    }

    /**
     * Checks if passed in {@code 'Throwable'} has given class in {@code 'cause'} hierarchy
     * <b>including</b> that throwable itself.
     *
     * <p>Note that this method follows includes {@link Throwable#getSuppressed()} into check.
     *
     * @param t   Throwable to check.
     * @param cls Cause classes to check.
     * @param msg Message text that should be in cause (if {@code null}, message won't be checked).
     * @return reference to the cause error if found, otherwise returns {@code null}.
     */
    public static <T extends Throwable> T cause(
            @NotNull Throwable t,
            @NotNull Class<T> cls,
            @Nullable String msg
    ) {
        for (Throwable th = t; th != null; th = th.getCause()) {
            if (cls.isAssignableFrom(th.getClass())) {
                if (msg != null) {
                    if (th.getMessage() != null && th.getMessage().contains(msg)) {
                        return (T) th;
                    } else {
                        continue;
                    }
                }

                return (T) th;
            }

            if (th.getCause() == th) {
                break;
            }
        }

        return null;
    }

    /**
     * Runs runnable task asyncronously.
     *
     * @param task Runnable.
     * @return Future with task result.
     */
    public static CompletableFuture<?> runAsync(final RunnableX task) {
        return runAsync(task, "async-runnable-runner");
    }

    /**
     * Runs runnable task asyncronously.
     *
     * @param task Runnable.
     * @return Future with task result.
     */
    public static CompletableFuture<?> runAsync(final RunnableX task, String threadName) {
        return runAsync(() -> {
            try {
                task.run();
            } catch (Throwable e) {
                throw new Exception(e);
            }

            return null;
        }, threadName);
    }

    /**
     * Runs callable task asyncronously.
     *
     * @param task Callable.
     * @return Future with task result.
     */
    public static <T> CompletableFuture<T> runAsync(final Callable<T> task) {
        return runAsync(task, "async-callable-runner");
    }

    /**
     * Runs callable task asyncronously.
     *
     * @param task Callable.
     * @param threadName Thread name.
     * @return Future with task result.
     */
    public static <T> CompletableFuture<T> runAsync(final Callable<T> task, String threadName) {
        final NamedThreadFactory thrFactory = new NamedThreadFactory(threadName, LOG);

        final CompletableFuture<T> fut = new CompletableFuture<T>();

        thrFactory.newThread(() -> {
            try {
                // Execute task.
                T res = task.call();

                fut.complete(res);
            } catch (Throwable e) {
                fut.completeExceptionally(e);
            }
        }).start();

        return fut;
    }

    /**
     * Runs callable tasks each in separate threads.
     *
     * @param calls Callable tasks.
     * @param threadFactory Thread factory.
     * @return Execution time in milliseconds.
     * @throws Exception If failed.
     */
    public static long runMultiThreaded(Iterable<Callable<?>> calls, ThreadFactory threadFactory) throws Exception {
        Collection<Thread> threads = new ArrayList<>();

        Collection<CompletableFuture<?>> futures = new ArrayList<>();

        for (Callable<?> task : calls) {
            CompletableFuture<?> fut = new CompletableFuture<>();

            futures.add(fut);

            threads.add(threadFactory.newThread(() -> {
                try {
                    // Execute task.
                    task.call();

                    fut.complete(null);
                } catch (Throwable e) {
                    fut.completeExceptionally(e);
                }
            }));
        }

        long time = System.currentTimeMillis();

        for (Thread t : threads) {
            t.start();
        }

        // Wait threads finish their job.
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            for (Thread t : threads) {
                t.interrupt();
            }

            throw e;
        }

        time = System.currentTimeMillis() - time;

        for (CompletableFuture<?> fut : futures) {
            fut.join();
        }

        return time;
    }

    /**
     * Runs callable tasks in specified number of threads.
     *
     * @param call Callable.
     * @param threadNum Number of threads.
     * @param threadName Thread names.
     * @return Execution time in milliseconds.
     * @throws Exception If failed.
     */
    public static long runMultiThreaded(Callable<?> call, int threadNum, String threadName) throws Exception {
        List<Callable<?>> calls = Collections.nCopies(threadNum, call);

        NamedThreadFactory threadFactory = new NamedThreadFactory(threadName, LOG);

        return runMultiThreaded(calls, threadFactory);
    }

    /**
     * Runs runnable object in specified number of threads.
     *
     * @param run Target runnable.
     * @param threadNum Number of threads.
     * @param threadName Thread name.
     * @return Future for the run. Future returns execution time in milliseconds.
     */
    public static CompletableFuture<Long> runMultiThreadedAsync(Runnable run, int threadNum, String threadName) {
        return runMultiThreadedAsync(() -> {
            run.run();

            return null;
        }, threadNum, threadName);
    }

    /**
     * Runs callable object in specified number of threads.
     *
     * @param call Callable.
     * @param threadNum Number of threads.
     * @param threadName Thread names.
     * @return Future for the run. Future returns execution time in milliseconds.
     */
    public static CompletableFuture<Long> runMultiThreadedAsync(Callable<?> call, int threadNum, final String threadName) {
        List<Callable<?>> calls = Collections.<Callable<?>>nCopies(threadNum, call);

        NamedThreadFactory threadFactory = new NamedThreadFactory(threadName, LOG);

        return runAsync(() -> runMultiThreaded(calls, threadFactory));
    }

    /**
     * Waits for the condition.
     *
     * @param cond Condition.
     * @param timeoutMillis Timeout in milliseconds.
     * @return {@code True} if the condition was satisfied within the timeout.
     * @throws InterruptedException If waiting was interrupted.
     */
    public static boolean waitForCondition(BooleanSupplier cond, long timeoutMillis) throws InterruptedException {
        return waitForCondition(cond, 50, timeoutMillis);
    }

    /**
     * Waits for the condition.
     *
     * @param cond Condition.
     * @param sleepMillis Sleep im milliseconds.
     * @param timeoutMillis Timeout in milliseconds.
     * @return {@code True} if the condition was satisfied within the timeout.
     * @throws InterruptedException If waiting was interrupted.
     */
    @SuppressWarnings("BusyWait")
    public static boolean waitForCondition(BooleanSupplier cond, long sleepMillis, long timeoutMillis) throws InterruptedException {
        long stop = System.currentTimeMillis() + timeoutMillis;

        while (System.currentTimeMillis() < stop) {
            if (cond.getAsBoolean()) {
                return true;
            }

            sleep(sleepMillis);
        }

        return false;
    }

    /**
     * Returns random BitSet.
     *
     * @param rnd  Random generator.
     * @param bits Amount of bits in bitset.
     * @return Random BitSet.
     */
    public static BitSet randomBitSet(Random rnd, int bits) {
        BitSet set = new BitSet();

        for (int i = 0; i < bits; i++) {
            if (rnd.nextBoolean()) {
                set.set(i);
            }
        }

        return set;
    }

    /**
     * Returns random byte array.
     *
     * @param rnd Random generator.
     * @param len Byte array length.
     * @return Random byte array.
     */
    public static byte[] randomBytes(Random rnd, int len) {
        byte[] data = new byte[len];
        rnd.nextBytes(data);

        return data;
    }

    /**
     * Returns random string.
     *
     * @param rnd Random generator.
     * @param len String length.
     * @return Random string.
     */
    public static String randomString(Random rnd, int len) {
        StringBuilder sb = new StringBuilder();

        while (sb.length() < len) {
            char pt = (char) rnd.nextInt(Character.MAX_VALUE + 1);

            if (Character.isDefined(pt)
                    && Character.getType(pt) != Character.PRIVATE_USE
                    && !Character.isSurrogate(pt)
            ) {
                sb.append(pt);
            }
        }

        return sb.toString();
    }

    /**
     * Creates a unique Ignite node name for the given test.
     *
     * <p>If the operating system is {@link #isWindowsOs Windows}, then the name will be short
     * due to the fact that the length of the paths must be up to 260 characters.
     *
     * @param testInfo Test info.
     * @param idx Node index.
     *
     * @return Node name.
     */
    public static String testNodeName(TestInfo testInfo, int idx) {
        String testMethodName = testInfo.getTestMethod().map(Method::getName).orElse("null");

        return isWindowsOs() ? shortTestMethodName(testMethodName) + "_" + idx :
                IgniteStringFormatter.format("{}_{}_{}",
                        testInfo.getTestClass().map(Class::getSimpleName).orElse("null"),
                        testMethodName,
                        idx);
    }

    /**
     * Runnable that could throw an exception.
     */
    @FunctionalInterface
    public interface RunnableX {
        void run() throws Throwable;
    }

    /**
     * Returns {@code true} if the operating system is Windows.
     */
    public static boolean isWindowsOs() {
        return System.getProperty("os.name").toLowerCase(Locale.US).contains("win");
    }

    /**
     * Returns a short version of the method name, such as "testShortMethodName" to "tsmn".
     *
     * @param methodName Method name for example "testShortMethodName".
     */
    public static String shortTestMethodName(String methodName) {
        assert !methodName.isBlank() : methodName;

        StringBuilder sb = new StringBuilder();

        char[] chars = methodName.trim().toCharArray();

        sb.append(chars[0]);

        for (int i = 1; i < chars.length; i++) {
            char c = chars[i];

            if (Character.isUpperCase(c)) {
                sb.append(c);
            }
        }

        return sb.toString().toLowerCase();
    }

    /**
     * Throw an exception as if it were unchecked.
     *
     * <p>This method erases type of the exception in the thrown clause, so checked exception could be thrown without need to wrap it with
     * unchecked one or adding a similar throws clause to the upstream methods.
     */
    @SuppressWarnings("unchecked")
    public static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }

    /**
     * Awaits completion of the given stage and returns its result.
     *
     * @param stage The stage.
     * @param timeout Maximum time to wait.
     * @param unit Time unit of the timeout argument.
     * @param <T> Type of the result returned by the stage.
     * @return A result of the stage.
     */
    @SuppressWarnings("UnusedReturnValue")
    public static <T> T await(CompletionStage<T> stage, long timeout, TimeUnit unit) {
        try {
            return stage.toCompletableFuture().get(timeout, unit);
        } catch (Throwable e) {
            if (e instanceof ExecutionException) {
                e = e.getCause();
            } else if (e instanceof CompletionException) {
                e = e.getCause();
            }

            sneakyThrow(e);
        }

        assert false;

        return null;
    }

    /**
     * Awaits completion of the given stage and returns its result.
     *
     * @param stage The stage.
     * @param <T> Type of the result returned by the stage.
     * @return A result of the stage.
     */
    @SuppressWarnings("UnusedReturnValue")
    public static <T> T await(CompletionStage<T> stage) {
        return await(stage, TIMEOUT_SEC, TimeUnit.SECONDS);
    }
}
