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

import java.nio.file.Path;
import java.util.function.Supplier;

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
     * Create a new instance from {@code pathSupplier} if it returns a nonempty value, otherwise use {@code defaultPathSupplier}.
     *
     * @param pathSupplier Path supplier.
     * @param defaultPathSupplier Default path supplier.
     */
    public static LazyPath create(Supplier<String> pathSupplier, Supplier<Path> defaultPathSupplier) {
        return new LazyPath(() -> pathOrDefault(pathSupplier.get(), defaultPathSupplier));
    }

    /**
     * Resolve the other path against this one.
     */
    public LazyPath resolveLazy(Path other) {
        return new LazyPath(() -> get().resolve(other));
    }

    /**
     * Resolve the other path against this one.
     */
    public LazyPath resolveLazy(String other) {
        return new LazyPath(() -> get().resolve(other));
    }

    @Override
    public Path get() {
        return super.get();
    }

    private static Path pathOrDefault(String value, Supplier<Path> defaultPathSupplier) {
        return value.isEmpty() ? defaultPathSupplier.get() : Path.of(value);
    }

    @Override
    public String toString() {
        return get().toString();
    }
}
