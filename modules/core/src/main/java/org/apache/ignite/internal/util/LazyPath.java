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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalException;

/**
 * A {@link Lazy} for {@link Path}.
 *
 * <p>Use this class to defer initialization of {@link Path} until it is needed.
 */
public class LazyPath extends Lazy<Path> {

    private LazyPath(Supplier<Path> supplier) {
        super(supplier);
    }

    /**
     * Create a new instance for a path.
     *
     * @param defaultPath Path.
     */
    public static LazyPath create(Path defaultPath) {
        return new LazyPath(() -> defaultPath);
    }

    /**
     * Create a new instance from {@code pathSupplier} if it returns a nonempty value, otherwise use {@code defaultPath}.
     *
     * @param pathSupplier Path supplier.
     * @param defaultPath Default path.
     */
    public static LazyPath create(Supplier<String> pathSupplier, Path defaultPath) {
        return new LazyPath(() -> pathOrDefault(pathSupplier.get(), defaultPath));
    }

    /**
     * Create a new instance from {@code pathSupplier} if it returns a nonempty value, otherwise use {@code defaultPathSupplier}.
     *
     * @param pathSupplier Path supplier.
     * @param defaultPathSupplier Default path supplier.
     */
    public static LazyPath create(Supplier<String> pathSupplier, Supplier<Path> defaultPathSupplier) {
        return create(pathSupplier, defaultPathSupplier, false);
    }

    /**
     * Create a new instance from {@code pathSupplier} if it returns a nonempty value, otherwise use {@code defaultPathSupplier}. If {code
     * ensureCreated} is {@code true}, then create the path if it doesn't exist.
     *
     * @param pathSupplier Path supplier.
     * @param defaultPathSupplier Default path supplier.
     * @param ensureCreated If {@code true}, then create the path if it doesn't exist.
     */
    public static LazyPath create(Supplier<String> pathSupplier, Supplier<Path> defaultPathSupplier, boolean ensureCreated) {
        return new LazyPath(() -> {
            Path path = pathOrDefault(pathSupplier.get(), defaultPathSupplier);

            return ensureCreated ? ensureCreated(path) : path;
        });
    }

    /**
     * Resolve the other path against this one.
     */
    public LazyPath resolveLazy(Path other) {
        return resolveLazy(other, false);
    }

    /**
     * Resolve the other path against this one.
     */
    public LazyPath resolveLazy(String other) {
        return resolveLazy(other, false);
    }

    /**
     * Resolve the other path against this one.  If {code ensureCreated} is {@code true}, then create the path if it doesn't exist.
     */
    public LazyPath resolveLazy(Path other, boolean ensureCreated) {
        return new LazyPath(() -> {
            Path resolved = get().resolve(other);

            return ensureCreated ? ensureCreated(resolved) : resolved;
        });
    }

    /**
     * Resolve the other path against this one. If {code ensureCreated} is {@code true}, then create the path if it doesn't exist.
     */
    public LazyPath resolveLazy(String other, boolean ensureCreated) {
        return new LazyPath(() -> {
            Path resolved = get().resolve(other);

            return ensureCreated ? ensureCreated(resolved) : resolved;
        });
    }

    @Override
    public Path get() {
        return super.get();
    }

    private static Path ensureCreated(Path dir) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new IgniteInternalException("Failed to create directory: " + dir + ": " + e.getMessage(), e);
        }

        return dir;
    }

    private static Path pathOrDefault(String value, Path defaultPath) {
        return value.isEmpty() ? defaultPath : Path.of(value);
    }

    private static Path pathOrDefault(String value, Supplier<Path> defaultPathSupplier) {
        return value.isEmpty() ? defaultPathSupplier.get() : Path.of(value);
    }

    @Override
    public String toString() {
        return get().toString();
    }
}
