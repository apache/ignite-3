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

package org.apache.ignite.internal.testframework;

import static java.lang.Thread.sleep;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.function.Function.identity;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.IgniteException;
import org.hamcrest.CustomMatcher;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.function.Executable;

/**
 * Utility class for tests.
 */
public final class IgniteTestUtils {
    private static final IgniteLogger LOG = Loggers.forClass(IgniteTestUtils.class);

    private static final int TIMEOUT_SEC = 30;

    /**
     * Set object field value via reflection.
     *
     * @param obj       Object to set field value to.
     * @param fieldName Field name to set value for.
     * @param val       New field value.
     * @throws IgniteInternalException In case of error.
     */
    public static void setFieldValue(Object obj, String fieldName, @Nullable Object val) throws IgniteInternalException {
        assert obj != null;
        assert fieldName != null;

        try {
            Class<?> cls = obj instanceof Class ? (Class<?>) obj : obj.getClass();

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

            if (!field.canAccess(obj)) {
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
    public static void setFieldValue(Object obj, Class<?> cls, String fieldName, Object val) throws IgniteInternalException {
        assert fieldName != null;

        try {
            Field field = cls.getDeclaredField(fieldName);

            if (!field.canAccess(obj)) {
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

            field.set(obj, val);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteInternalException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }

    /**
     * Finds a field in the given {@code target} object of the {@code declaredClass} type.
     *
     * @param target        target object from which to get field ({@code null} for static methods)
     * @param declaredClass class on which the field is declared
     * @param fieldName     name of the field
     * @return field
     */
    public static Field getField(@Nullable Object target, Class<?> declaredClass, String fieldName) {
        Field field;
        try {
            field = declaredClass.getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new IgniteInternalException("Did not find a field", e);
        }

        if (!field.canAccess(target)) {
            field.setAccessible(true);
        }

        return field;
    }

    /**
     * Returns field value.
     *
     * @param target        target object from which to get field value ({@code null} for static methods)
     * @param declaredClass class on which the field is declared
     * @param fieldName     name of the field
     * @return field value
     */
    public static <T> T getFieldValue(@Nullable Object target, Class<?> declaredClass, String fieldName) {
        try {
            return (T) getField(target, declaredClass, fieldName).get(target);
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
    public static <T> T getFieldValue(@Nullable Object obj, String... fieldNames) {
        assert obj != null;
        assert fieldNames != null;
        assert fieldNames.length >= 1;

        for (String fieldName : fieldNames) {
            Class<?> cls = obj instanceof Class ? (Class<?>) obj : obj.getClass();

            try {
                obj = getFieldValue(obj, cls, fieldName);
            } catch (IgniteInternalException e) {
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
     * Checks whether runnable throws exception, which is itself of a specified class.
     *
     * @param cls Expected exception class.
     * @param run Runnable to check.
     * @param errorMessageFragment Fragment of the error text in the expected exception, {@code null} if not to be checked.
     * @return Thrown throwable.
     */
    public static Throwable assertThrows(
            Class<? extends Throwable> cls,
            Executable run,
            @Nullable String errorMessageFragment
    ) {
        Throwable throwable = Assertions.assertThrows(cls, run);

        if (errorMessageFragment != null) {
            assertThat(throwable.getMessage(), containsString(errorMessageFragment));
        }

        return throwable;
    }

    /**
     * Checks whether runnable throws the correct {@link IgniteException}, which is itself of a specified class.
     *
     * @param expectedClass Expected exception class.
     * @param expectedErrorCode Expected error code of the {@link IgniteException}.
     * @param run Runnable to check.
     * @param errorMessageFragment Fragment of the error text in the expected exception, {@code null} if not to be checked.
     * @return Thrown throwable.
     */
    public static Throwable assertThrowsWithCode(
            Class<? extends IgniteException> expectedClass,
            int expectedErrorCode,
            Executable run,
            @Nullable String errorMessageFragment
    ) {
        try {
            run.execute();
        } catch (Throwable throwable) {
            try {
                assertInstanceOf(expectedClass, throwable);
            } catch (AssertionError err) {
                // An AssertionError from assertInstanceOf has nothing but a class name of the original exception.
                AssertionError assertionError = new AssertionError(err);

                assertionError.addSuppressed(throwable);

                throw assertionError;
            }

            IgniteException igniteException = (IgniteException) throwable;
            assertEquals(expectedErrorCode, igniteException.code(), "Invalid error code: " + igniteException.codeAsString());

            if (errorMessageFragment != null) {
                assertThat(throwable.getMessage(), containsString(errorMessageFragment));
            }

            return throwable;
        }

        throw new AssertionError("Exception has not been thrown.");
    }

    /**
     * Checks whether runnable throws exception, which is itself of a specified class, or has a cause of the specified class.
     *
     * @param run Runnable to check.
     * @param cls Expected exception class.
     * @return Thrown throwable.
     */
    public static Throwable assertThrowsWithCause(
            RunnableX run,
            Class<? extends Throwable> cls
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
            RunnableX run,
            Class<? extends Throwable> cls,
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
     * @param messageFragment Fragment that must be a substring of a cause message (if {@code null}, message won't be checked).
     * @return {@code True} if one of the causing exception is an instance of passed in classes, {@code false} otherwise.
     */
    public static boolean hasCause(
            Throwable t,
            Class<?> cls,
            @Nullable String messageFragment
    ) {
        for (Throwable th = t; th != null; th = th.getCause()) {
            if (cls.isAssignableFrom(th.getClass())) {
                if (messageFragment == null) {
                    return true;
                }

                if (th.getMessage() != null && th.getMessage().contains(messageFragment)) {
                    return true;
                }
            }

            for (Throwable n : th.getSuppressed()) {
                if (hasCause(n, cls, messageFragment)) {
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
    public static <T extends Throwable> @Nullable T cause(Throwable t, Class<T> cls) {
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
    public static <T extends Throwable> @Nullable T cause(
            Throwable t,
            Class<T> cls,
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
    public static CompletableFuture<Void> runAsync(RunnableX task) {
        return runAsync(task, "async-runnable-runner");
    }

    /**
     * Runs runnable task asyncronously.
     *
     * @param task Runnable.
     * @return Future with task result.
     */
    public static CompletableFuture<Void> runAsync(RunnableX task, String threadName) {
        return runAsync(() -> {
            try {
                task.run();
            } catch (Throwable e) {
                sneakyThrow(e);
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
    public static <T> CompletableFuture<T> runAsync(Callable<T> task) {
        return runAsync(task, "async-callable-runner");
    }

    /**
     * Runs callable task asyncronously.
     *
     * @param task Callable.
     * @param threadName Thread name.
     * @return Future with task result.
     */
    public static <T> CompletableFuture<T> runAsync(Callable<T> task, String threadName) {
        ThreadFactory thrFactory = IgniteThreadFactory.withPrefix(threadName, LOG, ThreadOperation.values());

        CompletableFuture<T> fut = new CompletableFuture<T>();

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
     * Executes an asynchronous operation in a thread that is allowed to execute any thread operation.
     *
     * @param operation Operation to execute.
     * @return Whatever the operation returns.
     */
    public static <T> CompletableFuture<T> bypassingThreadAssertionsAsync(Supplier<CompletableFuture<T>> operation) {
        return runAsync(operation::get).thenCompose(identity());
    }

    /**
     * Executes a synchronous operation in a thread that is allowed to execute any thread operation.
     *
     * @param operation Operation to execute.
     * @return Whatever the operation returns.
     */
    public static <T> T bypassingThreadAssertions(Supplier<T> operation) {
        return await(runAsync(operation::get));
    }

    /**
     * Executes a synchronous operation in a thread that is allowed to execute any thread operation.
     *
     * @param operation Operation to execute.
     */
    public static void bypassingThreadAssertions(Runnable operation) {
        await(runAsync(operation::run));
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
    public static CompletableFuture<Long> runMultiThreadedAsync(Callable<?> call, int threadNum, String threadName) {
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
        return waitForCondition(cond, 10, timeoutMillis);
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
     * @param testInfo Test info.
     * @param idx Node index.
     *
     * @return Node name.
     */
    public static String testNodeName(TestInfo testInfo, int idx) {
        String testMethodName = testInfo.getTestMethod().map(Method::getName).orElse("null");
        String testClassName = testInfo.getTestClass().map(Class::getSimpleName).orElse("null");

        return IgniteStringFormatter.format("{}_{}_{}",
                        shortTestMethodName(testClassName),
                        shortTestMethodName(testMethodName),
                        idx);
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
            // In tests, we generally interested in original exception,
            // rather than ExecutionException generated by the future's
            // contract. But topmost frame keeps important information
            // about failed operation in clients code. So, let's keep
            // this information in a kinda unusual yet sufficient for
            // further debug way
            Throwable original = ExceptionUtils.unwrapCause(e);

            if (original != e) {
                original.addSuppressed(new RuntimeException("This is a trimmed root"));
            }

            sneakyThrow(original);
        }

        throw new AssertionError("Should not get here");
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

    /**
     * {@link #runRace(long, RunnableX...)} with default timeout of 10 seconds.
     */
    public static void runRace(RunnableX... actions) {
        runRace(TimeUnit.SECONDS.toMillis(10), actions);
    }

    /**
     * Runs all actions, each in a separate thread, having a {@link CyclicBarrier} before calling {@link RunnableX#run()}.
     * Waits for threads completion or fails with the assertion if timeout exceeded.
     *
     * @throws AssertionError In case of timeout or if any of the runnables thrown an exception.
     */
    public static void runRace(long timeoutMillis, RunnableX... actions) {
        int length = actions.length;

        if (length == 0) {
            return; // Nothing to run.
        }

        CyclicBarrier barrier = new CyclicBarrier(length);

        Set<Throwable> throwables = ConcurrentHashMap.newKeySet();

        Thread[] threads = IntStream.range(0, length).mapToObj(i -> new Thread(() -> {
            try {
                barrier.await();

                actions[i].run();
            } catch (Throwable e) {
                throwables.add(e);
            }
        })).toArray(Thread[]::new);

        Stream.of(threads).forEach(Thread::start);

        long endTs = System.currentTimeMillis() + timeoutMillis;

        try {
            for (Thread thread : threads) {
                thread.join(Math.max(1, endTs - System.currentTimeMillis()));

                if (thread.isAlive()) {
                    throw new InterruptedException(); // Interrupt all actions and fail the race.
                }
            }
        } catch (InterruptedException e) {
            for (Thread thread : threads) {
                thread.interrupt();
            }

            fail("Race operations took too long.");
        }

        if (!throwables.isEmpty()) {
            AssertionError assertionError = new AssertionError("One or several threads have failed.");

            for (Throwable throwable : throwables) {
                assertionError.addSuppressed(throwable);
            }

            throw assertionError;
        }
    }

    /**
     * Returns a file system path for a resource name.
     *
     * @param cls A class.
     * @param resourceName A resource name.
     * @return A file system path matching the path component of the resource URL.
     */
    public static String getResourcePath(Class<?> cls, String resourceName) {
        return getPath(cls.getClassLoader().getResource(resourceName)).toString();
    }

    /**
     * Converts a URL to a file system path.
     *
     * <p>This method is needed to get a proper file system name on the Windows platform. For portable code
     * it should be used instead of URL::getPath().
     *
     * <p>For example, given a URL <i>file:///C:/dir/file.ext</i>, this method returns a Windows-specific
     * path: <i>C:\dir\file.ext</i>.
     *
     * <p>While the URL::getPath() method returns a path that will not work for file system API on Windows:
     * <i>/C:/dir/file.ext</i>.
     *
     * <p>There is no such problem on UNIX-like systems where given an URL <i>file:///dir/file.ext</i>,
     * both methods return the same result: <i>/dir/file.ext</i>
     *
     * @param url A resource URL.
     * @return A file system path matching the path component of the URL.
     */
    public static Path getPath(URL url) {
        try {
            return Path.of(url.toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e); // Shouldn't happen if the URL is obtained from the class loader.
        }
    }

    /**
     * Adds escape characters before backslashes in a path (on Windows), e.g. for the HOCON config parser.
     *
     * <p>For example, given a path argument <i>C:\dir\file.ext</i>, this method returns <i>C:\\dir\\file.ext</i>.
     *
     * @param path A path string.
     * @return A path string with escaped backslashes.
     */
    public static String escapeWindowsPath(String path) {
        return path.replace("\\", "\\\\");
    }

    /**
     * Generate file with dummy content with provided size.
     *
     * @param file File path.
     * @param fileSize File size in bytes.
     * @throws IOException if an I/O error is thrown.
     */
    public static void fillDummyFile(Path file, long fileSize) throws IOException {
        try (SeekableByteChannel channel = Files.newByteChannel(file, WRITE, CREATE)) {
            channel.position(fileSize - 4);

            ByteBuffer buf = ByteBuffer.allocate(4).putInt(2);
            buf.rewind();
            channel.write(buf);
        }
    }

    /**
     * Predicate matcher.
     *
     * @param <DataT> Data type.
     */
    public static class PredicateMatcher<DataT> extends CustomMatcher<DataT> {
        /** Predicate. */
        private final Predicate<DataT> predicate;

        /**
         * Constructor.
         *
         * @param pred Predicate.
         * @param msg Error description.
         */
        public PredicateMatcher(Predicate<DataT> pred, String msg) {
            super(msg);

            predicate = pred;
        }

        /** {@inheritDoc} */
        @Override
        public boolean matches(Object o) {
            return predicate.test((DataT) o);
        }
    }
}
