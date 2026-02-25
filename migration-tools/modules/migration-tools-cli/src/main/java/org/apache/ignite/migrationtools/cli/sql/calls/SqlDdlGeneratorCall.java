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

package org.apache.ignite.migrationtools.cli.sql.calls;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.migrationtools.config.Ignite2ConfigurationUtils;
import org.apache.ignite.migrationtools.sql.SqlDdlGenerator;
import org.apache.ignite.migrationtools.tablemanagement.TableTypeRegistryMapImpl;
import org.apache.ignite3.catalog.definitions.TableDefinition;
import org.apache.ignite3.internal.cli.core.call.Call;
import org.apache.ignite3.internal.cli.core.call.CallInput;
import org.apache.ignite3.internal.cli.core.call.CallOutput;
import org.apache.ignite3.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite3.internal.cli.core.exception.IgniteCliException;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Call for the SQL Generator command. */
public class SqlDdlGeneratorCall implements Call<SqlDdlGeneratorCall.Input, String> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDdlGeneratorCall.class);

    @Override
    public CallOutput<String> execute(Input i) {
        IgniteConfiguration cfg =
                Ignite2ConfigurationUtils.loadIgnite2Configuration(i.inputFilePath.toFile(), true, i.classloader);
        CacheConfiguration<?, ?>[] loadedCacheCfgs = Optional.ofNullable(cfg.getCacheConfiguration()).orElse(new CacheConfiguration[0]);
        if (loadedCacheCfgs.length == 0) {
            LOGGER.error("Config file does not have any cache configurations: {}", i.inputFilePath);
            return DefaultCallOutput.failure(
                    new IgniteCliException("Config file does not have any cache configurations: " + i.inputFilePath));
        }

        List<CacheConfiguration<?, ?>> availableCaches = Arrays.stream(loadedCacheCfgs)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        if (loadedCacheCfgs.length > availableCaches.size()) {
            var msg = String.format("Loaded %d caches out of %d. Check the errors for more information.", availableCaches.size(),
                    loadedCacheCfgs.length);
            if (i.stopOnError) {
                return DefaultCallOutput.failure(new IgniteCliException(msg));
            } else {
                LOGGER.warn(msg);
            }
        }

        SqlDdlGenerator
                generator = new SqlDdlGenerator(i.classloader, new TableTypeRegistryMapImpl(), i.allowExtraFields);

        List<TableDefinition> tableDefs = new ArrayList<>(availableCaches.size());
        for (CacheConfiguration<?, ?> cacheCfg : availableCaches) {
            try {
                TableDefinition def = generator.generateTableDefinition(cacheCfg);
                tableDefs.add(def);
            } catch (RuntimeException ex) {
                var msg = "Error while generating table definition for cache: " + cacheCfg.getName();
                if (i.stopOnError) {
                    return DefaultCallOutput.failure(new IgniteCliException(msg, ex));
                } else {
                    LOGGER.warn(msg, ex);
                }
            }
        }
        LOGGER.info("Found definitions for {} caches", tableDefs.size());

        String ddlScript = SqlDdlGenerator.createDdlQuery(tableDefs);
        LOGGER.info("Finished generating script for caches");

        if (i.targetFilePath == null) {
            return DefaultCallOutput.success(ddlScript);
        } else {
            try (var wr = Files.newBufferedWriter(i.targetFilePath, StandardCharsets.UTF_8)) {
                wr.write(ddlScript);
            } catch (IOException e) {
                LOGGER.error("Error writing to the target file ({}) for writing", i.targetFilePath, e);
                return DefaultCallOutput.failure(new IgniteCliException(
                        String.format("Error writing to the target file ({}) for writing", i.targetFilePath), e
                ));
            }

            return DefaultCallOutput.success("");
        }
    }

    /** Inputs. */
    public static class Input implements CallInput {
        private Path inputFilePath;

        @Nullable
        private Path targetFilePath;

        private boolean stopOnError;

        private boolean allowExtraFields;

        private ClassLoader classloader;

        /**
         * Constructor.
         *
         * @param inputFilePath Ignite 2 configuration file.
         * @param targetFilePath Optional file to save the DDL statements instead of stdout.
         * @param stopOnError Whether the command should stop on error or just skip caches with errors.
         * @param allowExtraFields If True, the extra fields column will be added to the tables.
         * @param classloader {@link ClassLoader} to use during the generation process. May be used to inject 3rd party libraries/classes.
         */
        public Input(Path inputFilePath, @Nullable Path targetFilePath, boolean stopOnError, boolean allowExtraFields,
                ClassLoader classloader) {
            this.inputFilePath = inputFilePath;
            this.targetFilePath = targetFilePath;
            this.stopOnError = stopOnError;
            this.allowExtraFields = allowExtraFields;
            this.classloader = classloader;
        }
    }
}
