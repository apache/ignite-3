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

package org.apache.ignite.internal.raft.util;

import java.nio.file.Path;
import java.util.function.Supplier;
import org.apache.ignite.configuration.ConfigurationValue;

/**
 * Utility class for reading path configuration values.
 */
public class ConfigurationPathUtils {

    public static Path pathOrDefault(String value, Supplier<Path> defaultPathSupplier) {
        return value.isEmpty() ? defaultPathSupplier.get() : Path.of(value);
    }

    public static Path pathOrDefault(String value, Path defaultPath) {
        return value.isEmpty() ? defaultPath : Path.of(value);
    }

    public static Path pathOrDefault(ConfigurationValue<String> value, Path defaultPath) {
        return pathOrDefault(value.value(), defaultPath);
    }

    public static Path pathOrDefault(ConfigurationValue<String> value, Supplier<Path> defaultPathSupplier) {
        return pathOrDefault(value.value(), defaultPathSupplier);
    }

    public static Supplier<Path> pathOrDefaultSupplier(ConfigurationValue<String> value, Supplier<Path> defaultPathSupplier) {
        return () -> pathOrDefault(value, defaultPathSupplier);
    }
}
