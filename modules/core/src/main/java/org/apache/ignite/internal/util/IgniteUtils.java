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

package org.apache.ignite.internal.util;

import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.util.worker.IgniteWorker;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringBuilder;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * Collection of utility methods used throughout the system.
 */
public class IgniteUtils {
    /** Byte bit-mask. */
    private static final int MASK = 0xf;

    /** The moment will be used as a start monotonic time. */
    private static final long BEGINNING_OF_TIME = System.nanoTime();

    /** Version of the JDK. */
    private static final String jdkVer = System.getProperty("java.specification.version");

    /** Class loader used to load Ignite. */
    private static final ClassLoader igniteClassLoader = IgniteUtils.class.getClassLoader();

    /** Indicates that assertions are enabled. */
    private static final boolean assertionsEnabled = IgniteUtils.class.desiredAssertionStatus();

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
     * @param <K>     Type of the map's keys.
     * @param <V>     Type of the map's values.
     * @return New map.
     */
    public static <K, V> HashMap<K, V> newHashMap(int expSize) {
        return new HashMap<>(capacity(expSize));
    }

    /**
     * Creates new {@link LinkedHashMap} with expected size.
     *
     * @param expSize Expected size of created map.
     * @param <K>     Type of the map's keys.
     * @param <V>     Type of the map's values.
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
     * Converts byte array to hex string.
     *
     * @param arr Array of bytes.
     * @return Hex string.
     */
    public static String toHexString(byte[] arr) {
        return toHexString(arr, Integer.MAX_VALUE);
    }

    /**
     * Converts byte array to hex string.
     *
     * @param arr    Array of bytes.
     * @param maxLen Maximum length of result string. Rounds down to a power of two.
     * @return Hex string.
     */
    public static String toHexString(byte[] arr, int maxLen) {
        assert maxLen >= 0 : "maxLem must be not negative.";

        int capacity = Math.min(arr.length << 1, maxLen);

        int lim = capacity >> 1;

        StringBuilder sb = new StringBuilder(capacity);

        for (int i = 0; i < lim; i++) {
            addByteAsHex(sb, arr[i]);
        }

        return sb.toString().toUpperCase();
    }

    /**
     * Returns hex representation of memory region.
     *
     * @param addr Pointer in memory.
     * @param len How much byte to read.
     */
    public static String toHexString(long addr, int len) {
        StringBuilder sb = new StringBuilder(len * 2);

        for (int i = 0; i < len; i++) {
            // Can not use getLong because on little-endian it produces wrong result.
            addByteAsHex(sb, GridUnsafe.getByte(addr + i));
        }

        return sb.toString();
    }

    /**
     * Returns hex representation of memory region.
     *
     * @param buf Buffer which content should be converted to string.
     */
    public static String toHexString(ByteBuffer buf) {
        StringBuilder sb = new StringBuilder(buf.capacity() * 2);

        for (int i = buf.position(); i < buf.limit(); i++) {
            // Can not use getLong because on little-endian it produces wrong result.
            addByteAsHex(sb, buf.get(i));
        }

        return sb.toString();
    }

    /**
     * Returns byte array represented by given hex string.
     *
     * @param s String containing a hex representation of bytes.
     * @return A byte array.
     */
    public static byte[] fromHexString(String s) {
        var len = s.length();

        assert (len & 1) == 0 : "length should be even";

        var data = new byte[len / 2];

        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }

        return data;
    }

    /**
     * Appends {@code byte} in hexadecimal format.
     *
     * @param sb String builder.
     * @param b  Byte to add in hexadecimal format.
     */
    private static void addByteAsHex(StringBuilder sb, byte b) {
        sb.append(Integer.toHexString(MASK & b >>> 4)).append(Integer.toHexString(MASK & b));
    }

    /**
     * Returns a hex string representation of the given long value.
     *
     * @param val Value to convert to string.
     * @return Hex string.
     */
    //TODO IGNITE-16350 Consider renaming or moving into other class.
    public static String hexLong(long val) {
        return new IgniteStringBuilder(16).appendHex(val).toString();
    }

    /**
     * Returns a hex string representation of the given integer value.
     *
     * @param val Value to convert to string.
     * @return Hex string.
     */
    //TODO IGNITE-16350 Consider renaming or moving into other class.
    public static String hexInt(int val) {
        return new IgniteStringBuilder(8).appendHex(val).toString();
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
     * @param ldr     Class loader.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(String clsName, @Nullable ClassLoader ldr) throws ClassNotFoundException {
        return forName(clsName, ldr, null);
    }

    /**
     * Gets class for provided name. Accepts primitive types names.
     *
     * @param clsName   Class name.
     * @param ldr       Class loader.
     * @param clsFilter Predicate to filter class names.
     * @return Class.
     * @throws ClassNotFoundException If class not found.
     */
    public static Class<?> forName(
            String clsName,
            @Nullable ClassLoader ldr,
            Predicate<String> clsFilter
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
     * Deletes a file or a directory with all sub-directories and files.
     *
     * @param path File or directory to delete.
     * @return {@code true} if the file or directory is successfully deleted or does not exist, {@code false} otherwise
     */
    public static boolean deleteIfExists(Path path) {
        try {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (exc != null) {
                        throw exc;
                    }

                    Files.delete(dir);

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);

                    return FileVisitResult.CONTINUE;
                }
            });

            return true;
        } catch (NoSuchFileException e) {
            return true;
        } catch (IOException e) {
            return false;
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
     * @param unit    the time unit of the timeout argument
     */
    public static void shutdownAndAwaitTermination(ExecutorService service, long timeout, TimeUnit unit) {
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
        AtomicReference<Exception> ex = new AtomicReference<>();

        closeables.filter(Objects::nonNull).forEach(closeable -> {
            try {
                closeable.close();
            } catch (Exception e) {
                if (!ex.compareAndSet(null, e)) {
                    ex.get().addSuppressed(e);
                }
            }
        });

        if (ex.get() != null) {
            throw ex.get();
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
     * Short date format pattern for log messages in "quiet" mode. Only time is included since we don't expect "quiet" mode to be used for
     * longer runs.
     */
    private static final DateTimeFormatter SHORT_DATE_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    /**
     * Prints stack trace of the current thread to provided logger.
     *
     * @param log Logger.
     * @param msg Message to print with the stack.
     * @deprecated Calls to this method should never be committed to master.
     */
    public static void dumpStack(IgniteLogger log, String msg, Object... params) {
        String reason = "Dumping stack";

        var err = new Exception(IgniteStringFormatter.format(msg, params));

        if (log != null) {
            log.warn(reason, err);
        } else {
            System.err.println("[" + LocalDateTime.now().format(SHORT_DATE_FMT) + "] (err) " + reason);

            err.printStackTrace(System.err);
        }
    }

    /**
     * Atomically moves or renames a file to a target file.
     *
     * @param sourcePath The path to the file to move.
     * @param targetPath The path to the target file.
     * @param log        Optional logger.
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
        } catch (final IOException e) {
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
            } catch (final IOException e1) {
                e1.addSuppressed(e);

                if (log != null) {
                    log.warn("Unable to move file. Going to delete source [sourcePath={}, targetPath={}]",
                            sourcePath,
                            targetPath
                    );
                }

                try {
                    Files.deleteIfExists(sourcePath);
                } catch (final IOException e2) {
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
    public static <O> O nonNullOrElse(O obj, O defaultObj) {
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
    public static <T> T getUninterruptibly(CompletableFuture<T> future) throws ExecutionException {
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
     * @param busyLock Component's busy lock
     * @param fn Function to run
     * @param <T> Type of returned value from {@code fn}
     * @return Result of the provided function
     */
    public static <T> T inBusyLock(IgniteSpinBusyLock busyLock, Supplier<T> fn) {
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
     * @param busyLock Component's busy lock
     * @param fn Runnable to run
     */
    public static void inBusyLock(IgniteSpinBusyLock busyLock, Runnable fn) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }
        try {
            fn.run();
        } finally {
            busyLock.leaveBusy();
        }
    }
}
