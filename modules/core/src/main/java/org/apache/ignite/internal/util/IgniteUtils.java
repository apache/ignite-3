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

import static java.util.Arrays.copyOfRange;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.internal.thread.ThreadAttributes;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.apache.ignite.internal.util.worker.IgniteWorker;
import org.jetbrains.annotations.Nullable;

/**
 * Collection of utility methods used throughout the system.
 */
public class IgniteUtils {
    /** The moment will be used as a start monotonic time. */
    private static final long BEGINNING_OF_TIME = System.nanoTime();

    /** Version of the JDK. */
    private static final String jdkVer = System.getProperty("java.specification.version");

    /** Class loader used to load Ignite. */
    private static final ClassLoader igniteClassLoader = IgniteUtils.class.getClassLoader();

    /** Indicates that assertions are enabled. */
    private static final boolean assertionsEnabled = IgniteUtils.class.desiredAssertionStatus();

    /** Alphanumeric with underscore regexp pattern. */
    private static final Pattern ALPHANUMERIC_UNDERSCORE_PATTERN = Pattern.compile("^[a-zA-Z_0-9]+$");

    /**
     * Gets the current monotonic time in milliseconds. This is the amount of milliseconds which passed from an arbitrary moment in the
     * past.
     */
    public static long monotonicMs() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - BEGINNING_OF_TIME);
    }

    /** Primitive class map. */
    private static final Map<String, Class<?>> primitiveMap = Map.of(
            "byte", byte.class,
            "short", short.class,
            "int", int.class,
            "long", long.class,
            "float", float.class,
            "double", double.class,
            "char", char.class,
            "boolean", boolean.class,
            "void", void.class
    );

    /** Class cache. */
    private static final ConcurrentMap<ClassLoader, ConcurrentMap<String, Class<?>>> classCache = new ConcurrentHashMap<>();

    /** Root package for JMX MBeans. */
    private static final String JMX_MBEAN_PACKAGE = "org.apache.ignite";

    /** Type attribute of {@link ObjectName} shared for all metric MBeans. */
    public static final String JMX_METRIC_GROUP_TYPE = "metrics";

    /** The error message displayed when attempting to convert a {@code null} value to a primitive data type. */
    public static final String NULL_TO_PRIMITIVE_ERROR_MESSAGE =
            "The value of field at index {} is null and cannot be converted to a primitive data type.";

    /** The error message displayed when attempting to convert a {@code null} value to a primitive data type. */
    public static final String NULL_TO_PRIMITIVE_NAMED_ERROR_MESSAGE =
            "The value of field '{}' is null and cannot be converted to a primitive data type.";

    /**
     * Get JDK version.
     *
     * @return JDK version.
     */
    public static String jdkVersion() {
        return jdkVer;
    }

    /**
     * Get major Java version from a string.
     *
     * @param verStr Version string.
     * @return Major version or zero if failed to resolve.
     */
    public static int majorJavaVersion(String verStr) {
        if (verStr == null || verStr.isEmpty()) {
            return 0;
        }

        try {
            String[] parts = verStr.split("\\.");

            int major = Integer.parseInt(parts[0]);

            if (parts.length == 1) {
                return major;
            }

            int minor = Integer.parseInt(parts[1]);

            return major == 1 ? minor : major;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * Returns the amount of RAM memory available on this machine.
     *
     * @return Total amount of memory in bytes or {@code -1} if any exception happened.
     */
    public static long getTotalMemoryAvailable() {
        MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();

        Object attr;

        try {
            attr = mbeanServer.getAttribute(
                    ObjectName.getInstance("java.lang", "type", "OperatingSystem"),
                    "TotalPhysicalMemorySize"
            );
        } catch (Exception e) {
            return -1;
        }

        return (attr instanceof Long) ? (Long) attr : -1;
    }

    /**
     * Returns a capacity that is sufficient to keep the map from being resized as long as it grows no larger than expSize and the load
     * factor is &gt;= its default (0.75).
     *
     * <p>Copy pasted from guava. See com.google.common.collect.Maps#capacity(int)
     *
     * @param expSize Expected size of the created map.
     * @return Capacity.
     */
    public static int capacity(int expSize) {
        if (expSize < 3) {
            return expSize + 1;
        }

        if (expSize < (1 << 30)) {
            return expSize + expSize / 3;
        }

        return Integer.MAX_VALUE; // any large value
    }

    /**
     * Creates new {@link HashMap} with expected size.
     *
     * @param expSize Expected size of the created map.
     * @param <K> Type of the map's keys.
     * @param <V> Type of the map's values.
     * @return New map.
     */
    public static <K, V> HashMap<K, V> newHashMap(int expSize) {
        return new HashMap<>(capacity(expSize));
    }

    /**
     * Creates new {@link HashSet} with expected size.
     *
     * @param expSize Expected size of the created set.
     * @param <E> the type of elements maintained by this set.
     * @return New map.
     */
    public static <E> HashSet<E> newHashSet(int expSize) {
        return new HashSet<>(capacity(expSize));
    }

    /**
     * Creates new {@link LinkedHashMap} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <K> Type of the map's keys.
     * @param <V> Type of the map's values.
     * @return New map.
     */
    public static <K, V> LinkedHashMap<K, V> newLinkedHashMap(int expSize) {
        return new LinkedHashMap<>(capacity(expSize));
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables, that otherwise encounter collisions for hashCodes that do not differ
     * in lower or upper bits.
     *
     * <p>This function has been taken from Java 8 ConcurrentHashMap with slightly modifications.
     *
     * @param h Value to hash.
     * @return Hash value.
     */
    public static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h << 15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h << 3);
        h ^= (h >>> 6);
        h += (h << 2) + (h << 14);

        return h ^ (h >>> 16);
    }

    /**
     * Applies a supplemental hash function to a given hashCode, which defends against poor quality hash functions.  This is critical
     * because ConcurrentHashMap uses power-of-two length hash tables, that otherwise encounter collisions for hashCodes that do not differ
     * in lower or upper bits.
     *
     * <p>This function has been taken from Java 8 ConcurrentHashMap with slightly modifications.
     *
     * @param obj Value to hash.
     * @return Hash value.
     */
    public static int hash(Object obj) {
        return hash(obj.hashCode());
    }

    /**
     * A primitive override of {@link #hash(Object)} to avoid unnecessary boxing.
     *
     * @param key Value to hash.
     * @return Hash value.
     */
    public static int hash(long key) {
        int val = (int) (key ^ (key >>> 32));

        return hash(val);
    }

    /**
     * Returns size in human-readable format.
     *
     * @param bytes Number of bytes to display.
     * @param si If {@code true}, then unit base is 1000, otherwise unit base is 1024.
     * @return Formatted size.
     */
    public static String readableSize(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;

        if (bytes < unit) {
            return bytes + " B";
        }

        int exp = (int) (Math.log(bytes) / Math.log(unit));

        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");

        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    /**
     * Gets absolute value for integer. If integer is {@link Integer#MIN_VALUE}, then {@code 0} is returned.
     *
     * @param i Integer.
     * @return Absolute value.
     */
    public static int safeAbs(int i) {
        return Math.max(Math.abs(i), 0);
    }

    /**
     * Gets absolute value for long. If long is {@link Long#MIN_VALUE}, then {@code 0} is returned.
     *
     * @param i Long value.
     * @return Absolute value.
     */
    public static long safeAbs(long i) {
        return Math.max(Math.abs(i), 0);
    }

    /**
     * Returns a first non-null value in a given array, if such is present.
     *
     * @param vals Input array.
     * @return First non-null value, or {@code null}, if array is empty or contains only nulls.
     */
    @SafeVarargs
    @Nullable
    public static <T> T firstNotNull(@Nullable T... vals) {
        if (vals == null) {
            return null;
        }

        for (T val : vals) {
            if (val != null) {
                return val;
            }
        }

        return null;
    }

    /**
     * Returns class loader used to load Ignite itself.
     *
     * @return Class loader used to load Ignite itself.
     */
    public static ClassLoader igniteClassLoader() {
        return igniteClassLoader;
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(String clsName, @Nullable ClassLoader ldr) throws ClassNotFoundException {
        return forName(clsName, ldr, null);
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName Class name.
     * @param ldr Class loader.
     * @param clsFilter Predicate to filter class names.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(
            String clsName,
            @Nullable ClassLoader ldr,
            @Nullable Predicate<String> clsFilter
    ) throws ClassNotFoundException {
        assert clsName != null;

        Class<?> cls = primitiveMap.get(clsName);

        if (cls != null) {
            return cls;
        }

        if (ldr == null) {
            ldr = igniteClassLoader;
        }

        ConcurrentMap<String, Class<?>> ldrMap = classCache.get(ldr);

        if (ldrMap == null) {
            ConcurrentMap<String, Class<?>> old = classCache.putIfAbsent(ldr, ldrMap = new ConcurrentHashMap<>());

            if (old != null) {
                ldrMap = old;
            }
        }

        cls = ldrMap.get(clsName);

        if (cls == null) {
            if (clsFilter != null && !clsFilter.test(clsName)) {
                throw new ClassNotFoundException("Deserialization of class " + clsName + " is disallowed.");
            }

            cls = Class.forName(clsName, true, ldr);

            Class<?> old = ldrMap.putIfAbsent(clsName, cls);

            if (old != null) {
                cls = old;
            }
        }

        return cls;
    }

    /**
     * Deletes a file or a directory with all sub-directories and files if exists.
     *
     * @param path File or directory to delete.
     * @return {@code true} if the file or directory is successfully deleted or does not exist, {@code false} otherwise
     */
    public static boolean deleteIfExists(Path path) {
        try {
            deleteIfExistsThrowable(path);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Deletes a file or a directory with all sub-directories and files if exists.
     *
     * @param path File or directory to delete.
     * @throws IOException if an I/O error is thrown by a visitor method
     */
    public static void deleteIfExistsThrowable(Path path) throws IOException {
        var visitor = new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                if (exc != null) {
                    throw exc;
                }

                Files.deleteIfExists(dir);

                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.deleteIfExists(file);

                return FileVisitResult.CONTINUE;
            }
        };

        try {
            Files.walkFileTree(path, visitor);
        } catch (NoSuchFileException ignored) {
            // Do nothing if file doesn't exist.
            // Using Files.exists() could lead to a race.
        }
    }

    /**
     * Calls fsync on a directory.
     *
     * @param dir Path to the directory.
     * @throws IOException If an I/O error occurs.
     */
    public static void fsyncDir(Path dir) throws IOException {
        assert Files.isDirectory(dir) : dir;

        // Fsync for directories doesn't work on Windows.
        if (OperatingSystem.current() == OperatingSystem.WINDOWS) {
            return;
        }

        try (FileChannel fc = FileChannel.open(dir, StandardOpenOption.READ)) {
            fc.force(true);
        }
    }

    /**
     * Calls fsync on a file.
     *
     * @param file Path to the file.
     * @throws IOException If an I/O error occurs.
     */
    public static void fsyncFile(Path file) throws IOException {
        assert Files.isRegularFile(file) : file;

        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.WRITE)) {
            fc.force(true);
        }
    }

    /**
     * Checks if assertions enabled.
     *
     * @return {@code true} if assertions enabled.
     */
    public static boolean assertionsEnabled() {
        return assertionsEnabled;
    }

    /**
     * Shuts down the given executor service gradually, first disabling new submissions and later, if necessary, cancelling remaining
     * tasks.
     *
     * <p>The method takes the following steps:
     *
     * <ol>
     *   <li>calls {@link ExecutorService#shutdown()}, disabling acceptance of new submitted tasks.
     *   <li>awaits executor service termination for half of the specified timeout.
     *   <li>if the timeout expires, it calls {@link ExecutorService#shutdownNow()}, cancelling
     *       pending tasks and interrupting running tasks.
     *   <li>awaits executor service termination for the other half of the specified timeout.
     * </ol>
     *
     * <p>If, at any step of the process, the calling thread is interrupted, the method calls {@link
     * ExecutorService#shutdownNow()} and returns.
     *
     * @param service the {@code ExecutorService} to shut down
     * @param timeout the maximum time to wait for the {@code ExecutorService} to terminate
     * @param unit the time unit of the timeout argument
     */
    public static void shutdownAndAwaitTermination(@Nullable ExecutorService service, long timeout, TimeUnit unit) {
        if (service == null) {
            return;
        }

        long halfTimeoutNanos = unit.toNanos(timeout) / 2;

        // Disable new tasks from being submitted
        service.shutdown();

        try {
            // Wait for half the duration of the timeout for existing tasks to terminate
            if (!service.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS)) {
                // Cancel currently executing tasks
                service.shutdownNow();
                // Wait the other half of the timeout for tasks to respond to being cancelled
                service.awaitTermination(halfTimeoutNanos, TimeUnit.NANOSECONDS);
            }
        } catch (InterruptedException ie) {
            // Preserve interrupt status
            Thread.currentThread().interrupt();
            // (Re-)Cancel if current thread also interrupted
            service.shutdownNow();
        }
    }

    /**
     * Closes all provided objects. If any of the {@link AutoCloseable#close} methods throw an exception, only the first thrown exception
     * will be propagated to the caller, after all other objects are closed, similar to the try-with-resources block.
     *
     * @param closeables Stream of objects to close.
     * @throws Exception If failed to close.
     */
    public static void closeAll(Stream<? extends AutoCloseable> closeables) throws Exception {
        AtomicReference<Throwable> ex = new AtomicReference<>();

        closeables.filter(Objects::nonNull).forEach(closeable -> {
            try {
                closeable.close();
            } catch (Throwable e) {
                if (!ex.compareAndSet(null, e)) {
                    ex.get().addSuppressed(e);
                }
            }
        });

        if (ex.get() != null) {
            throw ExceptionUtils.sneakyThrow(ex.get());
        }
    }

    /**
     * Closes all provided objects. If any of the {@link AutoCloseable#close} methods throw an exception, only the first thrown exception
     * will be propagated to the caller, after all other objects are closed, similar to the try-with-resources block.
     *
     * @param closeables Collection of objects to close.
     * @throws Exception If failed to close.
     */
    public static void closeAll(Collection<? extends AutoCloseable> closeables) throws Exception {
        closeAll(closeables.stream());
    }

    /**
     * Closes all provided objects.
     *
     * @param closeables Array of closeable objects to close.
     * @throws Exception If failed to close.
     * @see #closeAll(Collection)
     */
    public static void closeAll(AutoCloseable... closeables) throws Exception {
        closeAll(Arrays.stream(closeables));
    }

    /**
     * Closes all provided objects. If any of the {@link ManuallyCloseable#close} methods throw an exception, only the first thrown
     * exception will be propagated to the caller, after all other objects are closed, similar to the try-with-resources block.
     *
     * @param closeables Stream of objects to close.
     * @throws Exception If failed to close.
     */
    public static void closeAllManually(Stream<? extends ManuallyCloseable> closeables) throws Exception {
        AtomicReference<Throwable> ex = new AtomicReference<>();

        closeables.filter(Objects::nonNull).forEach(closeable -> {
            try {
                closeable.close();
            } catch (Throwable e) {
                if (!ex.compareAndSet(null, e)) {
                    ex.get().addSuppressed(e);
                }
            }
        });

        if (ex.get() != null) {
            throw ExceptionUtils.sneakyThrow(ex.get());
        }
    }

    /**
     * Closes all provided objects.
     *
     * @param closeables Collection of closeable objects to close.
     * @throws Exception If failed to close.
     * @see #closeAll(Collection)
     */
    public static void closeAllManually(Collection<? extends ManuallyCloseable> closeables) throws Exception {
        closeAllManually(closeables.stream());
    }

    /**
     * Closes all provided objects.
     *
     * @param closeables Array of closeable objects to close.
     * @throws Exception If failed to close.
     * @see #closeAll(Collection)
     */
    public static void closeAllManually(ManuallyCloseable... closeables) throws Exception {
        closeAllManually(Arrays.stream(closeables));
    }

    /**
     * Atomically moves or renames a file to a target file.
     *
     * @param sourcePath The path to the file to move.
     * @param targetPath The path to the target file.
     * @param log Optional logger.
     * @return The path to the target file.
     * @throws IOException If the source file cannot be moved to the target.
     */
    public static Path atomicMoveFile(Path sourcePath, Path targetPath, @Nullable IgniteLogger log) throws IOException {
        // Move temp file to target path atomically.
        // The code comes from
        // https://github.com/jenkinsci/jenkins/blob/master/core/src/main/java/hudson/util/AtomicFileWriter.java#L187
        Objects.requireNonNull(sourcePath, "sourcePath");
        Objects.requireNonNull(targetPath, "targetPath");

        Path success;

        try {
            success = Files.move(sourcePath, targetPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // If it falls here that can mean many things. Either that the atomic move is not supported,
            // or something wrong happened. Anyway, let's try to be over-diagnosing
            if (log != null) {
                if (e instanceof AtomicMoveNotSupportedException) {
                    log.info("Atomic move not supported. Falling back to non-atomic move [reason={}]", e.getMessage());
                } else {
                    log.info("Unable to move atomically. Falling back to non-atomic move [reason={}]", e.getMessage());
                }

                if (targetPath.toFile().exists() && log.isInfoEnabled()) {
                    log.info("The target file already exists [path={}]", targetPath);
                }
            }

            try {
                success = Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e1) {
                e1.addSuppressed(e);

                if (log != null) {
                    log.warn("Unable to move file. Going to delete source [sourcePath={}, targetPath={}]",
                            sourcePath,
                            targetPath
                    );
                }

                try {
                    Files.deleteIfExists(sourcePath);
                } catch (IOException e2) {
                    e2.addSuppressed(e1);

                    if (log != null) {
                        log.warn("Unable to delete file [path={}]", sourcePath);
                    }

                    throw e2;
                }

                throw e1;
            }
        }

        return success;
    }

    /**
     * Return {@code obj} if not null, otherwise {@code defaultObj}.
     *
     * @param obj Object.
     * @param defaultObj Default object.
     * @param <O> Object type.
     * @return Object or default object.
     */
    public static <O> O nonNullOrElse(@Nullable O obj, O defaultObj) {
        return (obj != null) ? obj : defaultObj;
    }

    /**
     * Returns {@code true} If the given value is power of 2 (0 is not power of 2).
     *
     * @param i Value.
     */
    public static boolean isPow2(int i) {
        return i > 0 && (i & (i - 1)) == 0;
    }

    /**
     * Returns {@code true} If the given value is power of 2 (0 is not power of 2).
     *
     * @param i Value.
     */
    public static boolean isPow2(long i) {
        return i > 0 && (i & (i - 1)) == 0;
    }

    /**
     * Waits if necessary for this future to complete, and then returns its result ignoring interrupts.
     *
     * @return Result value.
     * @throws CancellationException If this future was cancelled.
     * @throws ExecutionException If this future completed exceptionally.
     */
    public static <T> T getUninterruptibly(Future<T> future) throws ExecutionException {
        boolean interrupted = false;

        try {
            while (true) {
                try {
                    return future.get();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Waits if necessary for this future to complete, and then returns its result ignoring interrupts.
     *
     * @param future Future to wait on.
     * @param timeout Timeout in milliseconds.
     * @return Result value.
     * @throws CancellationException If this future was cancelled.
     * @throws ExecutionException If this future completed exceptionally.
     */
    public static <T> T getUninterruptibly(Future<T> future, long timeout) throws ExecutionException, TimeoutException {
        boolean interrupted = false;

        try {
            long start = System.currentTimeMillis();
            while (true) {
                long current = System.currentTimeMillis();
                long wait = timeout - (current - start);
                if (wait < 0) {
                    throw new TimeoutException("Timeout waiting for future completion [timeout=" + timeout + "ms]");
                }

                try {
                    return future.get(wait, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Blocks until the future is completed and either returns the value from the normal completion, or throws an exception if the future
     * was completed exceptionally. The exception might be wrapped in a copy preserving the error code.
     *
     * <p>The wait is interruptible. That is, the thread can be interrupted; in such case, {@link InterruptedException} is thrown sneakily.
     *
     * @param future Future to wait on.
     * @return Value from the future.
     */
    public static <T> T getInterruptibly(Future<T> future) {
        try {
            return future.get();
        } catch (ExecutionException e) {
            throw ExceptionUtils.sneakyThrow(ExceptionUtils.copyExceptionWithCause(e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw ExceptionUtils.sneakyThrow(e);
        }
    }

    /**
     * Stops workers from given collection and waits for their completion.
     *
     * @param workers Workers collection.
     * @param cancel Whether it should cancel workers.
     * @param log Logger.
     */
    public static void awaitForWorkersStop(Collection<IgniteWorker> workers, boolean cancel, @Nullable IgniteLogger log) {
        if (cancel) {
            workers.forEach(IgniteWorker::cancel);
        }

        for (IgniteWorker worker : workers) {
            try {
                worker.join();
            } catch (Exception e) {
                if (log != null && log.isWarnEnabled()) {
                    log.debug("Unable to cancel ignite worker [worker={}, reason={}]", worker.toString(), e.getMessage());
                }
            }
        }
    }

    /**
     * Method that runs the provided {@code fn} in {@code busyLock}.
     *
     * @param busyLock Component's busy lock.
     * @param fn Function to run.
     * @param <T> Type of returned value from {@code fn}.
     * @return Result of the provided function.
     * @throws IgniteInternalException with cause {@link NodeStoppingException} if {@link IgniteBusyLock#enterBusy()} failed.
     */
    public static <T> T inBusyLock(IgniteBusyLock busyLock, Supplier<T> fn) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }
        try {
            return fn.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Method that runs the provided {@code fn} in {@code busyLock}.
     *
     * @param busyLock Component's busy lock.
     * @param fn Function to run.
     * @return Result of the provided function.
     * @throws IgniteInternalException with cause {@link NodeStoppingException} if {@link IgniteBusyLock#enterBusy()} failed.
     */
    public static int inBusyLock(IgniteBusyLock busyLock, IntSupplier fn) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }
        try {
            return fn.getAsInt();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Method that runs the provided {@code fn} in {@code busyLock}.
     *
     * @param busyLock Component's busy lock.
     * @param fn Runnable to run.
     * @throws IgniteInternalException with cause {@link NodeStoppingException} if {@link IgniteBusyLock#enterBusy()} failed.
     */
    public static void inBusyLock(IgniteBusyLock busyLock, Runnable fn) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }
        try {
            fn.run();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Method that runs the provided {@code fn} in {@code busyLock}.
     *
     * @param <T> Type of returned value from {@code fn}.
     * @param busyLock Component's busy lock.
     * @param fn Function to run.
     * @return Future returned from the {@code fn}, or future with the {@link NodeStoppingException} if
     *         {@link IgniteBusyLock#enterBusy()} failed or with runtime exception/error while executing the {@code fn}.
     */
    public static <T> CompletableFuture<T> inBusyLockAsync(IgniteBusyLock busyLock, Supplier<CompletableFuture<T>> fn) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            return fn.get();
        } catch (Throwable t) {
            return failedFuture(t);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Method that runs the provided {@code fn} in {@code busyLock} if {@link IgniteBusyLock#enterBusy()} succeed. Otherwise it just
     * silently returns.
     *
     * @param busyLock Component's busy lock.
     * @param fn Runnable to run.
     */
    public static void inBusyLockSafe(IgniteBusyLock busyLock, Runnable fn) {
        if (!busyLock.enterBusy()) {
            return;
        }
        try {
            fn.run();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Collects all the fields of given class which are defined as a public static within the specified class.
     *
     * @param sourceCls The class to lookup fields in.
     * @param targetCls Type of the fields of interest.
     * @return A mapping name to property itself.
     */
    public static <T> List<T> collectStaticFields(Class<?> sourceCls, Class<? extends T> targetCls) {
        List<T> result = new ArrayList<>();

        for (Field f : sourceCls.getDeclaredFields()) {
            if (!targetCls.equals(f.getType())
                    || !Modifier.isStatic(f.getModifiers())
                    || !Modifier.isPublic(f.getModifiers())) {
                continue;
            }

            try {
                T value = targetCls.cast(f.get(sourceCls));

                result.add(value);
            } catch (IllegalAccessException e) {
                // should not happen
                throw new AssertionError(e);
            }
        }

        return result;
    }

    /**
     * Cancels the future and runs a consumer on future's result if it was completed before the cancellation. Does nothing if future is
     * cancelled or completed exceptionally.
     *
     * @param future Future.
     * @param consumer Consumer that accepts future's result.
     * @param <T> Future's result type.
     */
    public static <T> void cancelOrConsume(CompletableFuture<T> future, Consumer<T> consumer) {
        future.cancel(true);

        consumeIfFinishedSuccessfully(future, consumer);
    }

    /**
     * Fails the future and runs a consumer on future's result if it was completed before being failed. Does nothing if future is
     * cancelled or completed exceptionally.
     *
     * @param future Future.
     * @param failure With what to fail the future.
     * @param consumer Consumer that accepts future's result.
     * @param <T> Future's result type.
     */
    public static <T> void failOrConsume(CompletableFuture<T> future, Throwable failure, Consumer<T> consumer) {
        future.completeExceptionally(failure);

        consumeIfFinishedSuccessfully(future, consumer);
    }

    private static <T> void consumeIfFinishedSuccessfully(CompletableFuture<T> future, Consumer<T> consumer) {
        if (future.isCancelled() || future.isCompletedExceptionally()) {
            return;
        }

        assert future.isDone();

        T res = future.join();

        assert res != null;

        consumer.accept(res);
    }

    /**
     * Constructs JMX object name with the given properties.
     *
     * @param nodeName Ignite node name.
     * @param group Name of the group.
     * @param name Name of mbean.
     *
     * @return JMX object name.
     * @throws MalformedObjectNameException Thrown in case of any errors.
     */
    public static ObjectName makeMbeanName(
            @Nullable String nodeName,
            @Nullable String group,
            String name
    ) throws MalformedObjectNameException {
        var sb = new StringBuilder(JMX_MBEAN_PACKAGE + ':');

        if (nodeName != null && !nodeName.isEmpty()) {
            sb.append("nodeName=").append(nodeName).append(',');
        }

        sb.append("type=").append(JMX_METRIC_GROUP_TYPE).append(',');

        if (group != null && !group.isEmpty()) {
            sb.append("group=").append(escapeObjectNameValue(group)).append(',');

            if (name.startsWith(group)) {
                name = name.substring(group.length() + 1);
            }
        }

        sb.append("name=").append(escapeObjectNameValue(name));

        return new ObjectName(sb.toString());
    }

    /**
     * Escapes the given string to be used as a value in the ObjectName syntax.
     *
     * @param s A string to be escape.
     * @return An escaped string.
     */
    private static String escapeObjectNameValue(String s) {
        if (alphanumericUnderscore(s)) {
            return s;
        }

        return '\"' + s.replaceAll("[\\\\\"?*]", "\\\\$0") + '\"';
    }

    /**
     * Returns {@code true} when the given string contains only alphanumeric and underscore symbols, {@code false} otherwise.
     *
     * @param s String to check.
     * @return {@code true} if given string contains only alphanumeric and underscore symbols.
     */
    public static boolean alphanumericUnderscore(String s) {
        return ALPHANUMERIC_UNDERSCORE_PATTERN.matcher(s).matches();
    }

    /**
     * Filter the collection using the given predicate.
     *
     * @param collection Collection.
     * @param predicate Predicate.
     * @return Filtered list.
     */
    public static <T> List<T> filter(Collection<T> collection, Predicate<T> predicate) {
        List<T> result = new ArrayList<>();

        for (T e : collection) {
            if (predicate.test(e)) {
                result.add(e);
            }
        }

        return result;
    }

    /**
     * Find the first element in the given list.
     *
     * @param list List.
     * @return Optional containing element (if present).
     */
    public static <T> Optional<T> findFirst(List<T> list) {
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    /**
     * Find any element in given collection.
     *
     * @param collection Collection.
     * @return Optional containing element (if present).
     */
    public static <T> Optional<T> findAny(Collection<T> collection) {
        return findAny(collection, null);
    }

    /**
     * Find any element in given collection for which the predicate returns {@code true}.
     *
     * @param collection Collection.
     * @param predicate Predicate.
     * @return Optional containing element (if present).
     */
    public static <T> Optional<T> findAny(Collection<T> collection, @Nullable Predicate<T> predicate) {
        if (!collection.isEmpty()) {
            for (Iterator<T> it = collection.iterator(); it.hasNext(); ) {
                T t = it.next();

                if (predicate == null || predicate.test(t)) {
                    return Optional.ofNullable(t);
                }
            }
        }

        return Optional.empty();
    }

    /**
     * Iterates over the given collection and applies the given closure to each element using the collection element and its index.
     *
     * @param collection Collection.
     * @param closure Closure to apply.
     * @param <T> Type of collection element.
     */
    public static <T> void forEachIndexed(Collection<T> collection, BiConsumer<T, Integer> closure) {
        int i = 0;

        for (T t : collection) {
            closure.accept(t, i++);
        }
    }

    /**
     * Retries operation until it succeeds or fails with exception that is different than the given.
     *
     * @param operation Operation.
     * @param stopRetryCondition Condition that accepts the exception if one has been thrown, and defines whether retries should be
     *         stopped.
     * @param executor Executor to make retry in.
     * @return Future that is completed when operation is successful or failed with other exception than the given.
     */
    public static <T> CompletableFuture<T> retryOperationUntilSuccess(
            Supplier<CompletableFuture<T>> operation,
            Function<Throwable, Boolean> stopRetryCondition,
            Executor executor
    ) {
        CompletableFuture<T> fut = new CompletableFuture<>();

        retryOperationUntilSuccess(operation, stopRetryCondition, fut, executor);

        return fut;
    }

    /**
     * Retries operation until it succeeds or fails with exception that is different than the given.
     *
     * @param operation Operation.
     * @param stopRetryCondition Condition that accepts the exception if one has been thrown, and defines whether retries should be
     *         stopped.
     * @param executor Executor to make retry in.
     * @param fut Future that is completed when operation is successful or failed with other exception than the given.
     */
    public static <T> void retryOperationUntilSuccess(
            Supplier<CompletableFuture<T>> operation,
            Function<Throwable, Boolean> stopRetryCondition,
            CompletableFuture<T> fut,
            Executor executor
    ) {
        operation.get()
                .whenComplete((res, e) -> {
                    if (e == null) {
                        fut.complete(res);
                    } else {
                        if (stopRetryCondition.apply(e)) {
                            fut.completeExceptionally(e);
                        } else {
                            executor.execute(() -> retryOperationUntilSuccess(operation, stopRetryCondition, fut, executor));
                        }
                    }
                });
    }

    /**
     * Retries operation until it succeeds or timeout occurs.
     *
     * @param operation Operation.
     * @param timeout Timeout in milliseconds.
     * @param executor Executor to make retry in.
     * @return Future that is completed when operation is successful or failed with other exception than the given.
     */
    public static <T> CompletableFuture<T> retryOperationUntilSuccessOrTimeout(
            Supplier<CompletableFuture<T>> operation,
            long timeout,
            Executor executor
    ) {
        CompletableFuture<T> futureWithTimeout = new CompletableFuture<T>().orTimeout(timeout, TimeUnit.MILLISECONDS);

        retryOperationUntilSuccessOrFutureDone(operation, futureWithTimeout, executor);

        return futureWithTimeout;
    }

    /**
     * Retries operation until it succeeds or provided future is done.
     *
     * @param operation Operation.
     * @param future Future to track.
     * @param executor Executor to make retry in.
     */
    private static <T> void retryOperationUntilSuccessOrFutureDone(
            Supplier<CompletableFuture<T>> operation,
            CompletableFuture<T> future,
            Executor executor
    ) {

        operation.get()
                .whenComplete((res, e) -> {
                    if (!future.isDone()) {
                        if (e == null) {
                            future.complete(res);
                        } else {
                            executor.execute(() -> retryOperationUntilSuccessOrFutureDone(operation, future, executor));
                        }
                    }
                });
    }

    /**
     * Utility method to check if one byte array starts with a specified sequence of bytes.
     *
     * @param key The array to check.
     * @param prefix The prefix bytes to test for.
     * @return {@code true} if the key starts with the bytes from the prefix.
     */
    public static boolean startsWith(byte[] key, byte[] prefix) {
        return key.length >= prefix.length
                && Arrays.equals(key, 0, prefix.length, prefix, 0, prefix.length);
    }

    /**
     * Serializes collection to bytes.
     *
     * @param collection Collection.
     * @param transform Transform function for the collection element.
     * @return Byte array.
     */
    public static <T> byte[] collectionToBytes(Collection<T> collection, Function<T, byte[]> transform) {
        int bytesObjects = 0;
        List<byte[]> objects = new ArrayList<>();

        for (T o : collection) {
            byte[] b = transform.apply(o);
            objects.add(b);
            bytesObjects += b.length;
        }

        bytesObjects += Integer.BYTES;
        ByteBuffer buf = ByteBuffer.allocate(bytesObjects).order(ByteOrder.LITTLE_ENDIAN);

        buf.putInt(objects.size());

        for (byte[] o : objects) {
            buf.put(o);
        }

        return buf.array();
    }

    /**
     * Deserializes the list from byte buffer. Requires little-endian byte order.
     *
     * @param buf Byte buffer.
     * @param transform Transform function to create list element.
     * @return List.
     */
    public static <T> List<T> bytesToList(ByteBuffer buf, Function<ByteBuffer, T> transform) {
        int length = buf.getInt();
        assert length >= 0 : "Negative collection size: " + length;

        List<T> result = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            assert buf.position() < buf.limit() : "Can't read an object from the given buffer [position=" + buf.position()
                    + ", limit=" + buf.limit() + ']';

            result.add(transform.apply(buf));
        }

        return result;
    }

    /**
     * Converts byte buffer into a byte array. The content of the array is all bytes from the position "0" to "capacity". Preserves original
     * position/limit values in the buffer. Always returns a new instance, instead of accessing the internal buffer's array.
     */
    public static byte[] byteBufferToByteArray(ByteBuffer buffer) {
        if (buffer.hasArray()) {
            int offset = buffer.arrayOffset();

            return copyOfRange(buffer.array(), offset, offset + buffer.capacity());
        } else {
            byte[] array = new byte[buffer.capacity()];

            int originalPosition = buffer.position();

            buffer.position(0);
            buffer.get(array);

            buffer.position(originalPosition);

            return array;
        }
    }

    /**
     * Makes array from the given arguments, if any argument is an array itself, its elements are added into result instead of it.
     *
     * @param arguments Arguments.
     * @return Flat array.
     */
    public static Object[] flatArray(Object... arguments) {
        List<Object> list = new ArrayList<>();

        for (Object arg : arguments) {
            if (arg != null && arg.getClass().isArray()) {
                int length = Array.getLength(arg);
                for (int i = 0; i < length; i++) {
                    list.add(Array.get(arg, i));
                }
            } else {
                list.add(arg);
            }
        }

        return list.toArray();
    }

    private static CompletableFuture<Void> startAsync(ComponentContext componentContext, Stream<? extends IgniteComponent> components) {
        return allOf(components
                .filter(Objects::nonNull)
                .map(component -> component.startAsync(componentContext))
                .toArray(CompletableFuture[]::new));
    }

    /**
     * Asynchronously starts all ignite components.
     *
     * @param componentContext Ignite component context.
     * @param components Array of ignite components to start, may contain {@code null} elements.
     * @return CompletableFuture that will be completed when all components are started.
     */
    public static CompletableFuture<Void> startAsync(ComponentContext componentContext, @Nullable IgniteComponent... components) {
        return startAsync(componentContext, Stream.of(components));
    }

    /**
     * Asynchronously starts all ignite components.
     *
     * @param componentContext Ignite component context.
     * @param components Collection of ignite components to start.
     * @return CompletableFuture that will be completed when all components are started.
     */
    public static CompletableFuture<Void> startAsync(ComponentContext componentContext, Collection<? extends IgniteComponent> components) {
        return startAsync(componentContext, components.stream());
    }

    private static CompletableFuture<Void> stopAsync(ComponentContext componentContext, Stream<? extends IgniteComponent> components) {
        return allOf(components
                .filter(Objects::nonNull)
                .map(igniteComponent -> {
                    try {
                        return igniteComponent.stopAsync(componentContext);
                    } catch (Throwable e) {
                        // Make sure a failure in the synchronous part will not interrupt the stopping process of other components.
                        return failedFuture(e);
                    }
                })
                .toArray(CompletableFuture[]::new));
    }

    /**
     * Asynchronously exec all stop functions.
     *
     * @param components Array of stop functions.
     * @return CompletableFuture that will be completed when all components are stopped.
     */
    public static CompletableFuture<Void> stopAsync(Supplier<CompletableFuture<Void>>... components) {
        return allOf(Stream.of(components)
                .filter(Objects::nonNull)
                .map(igniteComponent -> {
                    try {
                        return igniteComponent.get();
                    } catch (Throwable e) {
                        // Make sure a failure in the synchronous part will not interrupt the stopping process of other components.
                        return failedFuture(e);
                    }
                })
                .toArray(CompletableFuture[]::new));
    }

    /**
     * Asynchronously stops all ignite components.
     *
     * @param componentContext Ignite component context.
     * @param components Array of ignite components to stop, may contain {@code null} elements.
     * @return CompletableFuture that will be completed when all components are stopped.
     */
    public static CompletableFuture<Void> stopAsync(ComponentContext componentContext, @Nullable IgniteComponent... components) {
        return stopAsync(componentContext, Stream.of(components));
    }

    /**
     * Asynchronously stops all ignite components.
     *
     * @param componentContext Ignite component context.
     * @param components Collection of ignite components to stop.
     * @return CompletableFuture that will be completed when all components are stopped.
     */
    public static CompletableFuture<Void> stopAsync(ComponentContext componentContext, Collection<? extends IgniteComponent> components) {
        return stopAsync(componentContext, components.stream());
    }

    /**
     * The method checks the list of allowed operations in the current thread and returns false if the thread is fit to continue or true if
     * we must switch to another.
     *
     * @param requiredOperationPermissions Set of thread operations that have to be supported by the current thread.
     * @return True if we have to switch to a specific pool, otherwise we can continue in the current thread.
     */
    public static boolean shouldSwitchToRequestsExecutor(ThreadOperation... requiredOperationPermissions) {
        if (Thread.currentThread() instanceof ThreadAttributes) {
            ThreadAttributes thread = (ThreadAttributes) Thread.currentThread();

            for (ThreadOperation op : requiredOperationPermissions) {
                if (!thread.allows(op)) {
                    return true;
                }
            }

            return false;
        } else {
            if (PublicApiThreading.executingSyncPublicApi()) {
                // It's a user thread, it executes a sync public API call, so it can do anything, no switch is needed.
                return false;
            }
            if (PublicApiThreading.executingAsyncPublicApi()) {
                // It's a user thread, it executes an async public API call, so it cannot do anything, a switch is needed.
                return true;
            }

            // It's something else: either a JRE thread or an Ignite thread not marked with ThreadAttributes. As we are not sure,
            // let's switch: false negative can produce assertion errors.
            return true;
        }
    }

    /** Throws the {@link NullPointerException} if the provided tuple has a {@code null} value at the specified index. */
    public static void ensureNotNull(InternalTuple tuple, int fieldIndex, int displayIndex) {
        if (tuple.hasNullValue(fieldIndex)) {
            throw new NullPointerException(IgniteStringFormatter.format(NULL_TO_PRIMITIVE_ERROR_MESSAGE, displayIndex));
        }
    }

    /** Throws the {@link NullPointerException} if the provided tuple has a {@code null} value at the specified index. */
    public static void ensureNotNull(InternalTuple tuple, int fieldIndex, String fieldName) {
        if (tuple.hasNullValue(fieldIndex)) {
            throw new NullPointerException(IgniteStringFormatter.format(NULL_TO_PRIMITIVE_NAMED_ERROR_MESSAGE, fieldName));
        }
    }
}
