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
 */
public class LazyPath extends Lazy<Path> {
    /**
     * Creates the lazy value with the given value supplier.
     *
     * @param supplier A supplier of the value.
     */
    private LazyPath(Supplier<Path> supplier) {
        super(supplier);
    }

    /**
     * Create a new instance.
     */
    public static LazyPath create(Path defaultPath) {
        return new LazyPath(() -> defaultPath);
    }

    /**
     * Create a new instance.
     */
    public static LazyPath create(Supplier<String> pathConfig, Path defaultPath) {
        return new LazyPath(() -> pathOrDefault(pathConfig.get(), defaultPath));
    }

    /**
     * Create a new instance.
     */
    public static LazyPath create(Supplier<String> pathConfig, Supplier<Path> defaultPathSupplier) {
        return create(pathConfig, defaultPathSupplier, false);
    }

    /**
     * Create a new instance.
     */
    public static LazyPath create(Supplier<String> pathConfig, Supplier<Path> defaultPathSupplier, boolean ensureCreated) {
        return new LazyPath(() -> {
            Path path = pathOrDefault(pathConfig.get(), defaultPathSupplier);

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
     *
     */
    public LazyPath resolveLazy(String other) {
        return resolveLazy(other, false);
    }

    /**
     * Resolve the other path against this one.
     *
     */
    public LazyPath resolveLazy(Path other, boolean ensureCreated) {
        return new LazyPath(() -> {
            Path resolved = get().resolve(other);

            return ensureCreated ? ensureCreated(resolved) : resolved;
        });
    }

    /**
     * Resolve the other path against this one.
     *
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
