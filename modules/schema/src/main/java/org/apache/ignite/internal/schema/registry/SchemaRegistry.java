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

package org.apache.ignite.internal.schema.registry;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Holds schema descriptors for actual schema versions.
 * <p>
 * Schemas MUST be registered in a version ascending order incrementing by {@code 1} with NO gaps,
 * otherwise an exception will be thrown. The version numbering starts from the {@code 1}.
 * <p>
 * After some table maintenance process some first versions may become outdated and can be safely cleaned up
 * if the process guarantees the table no longer has a data of these versions.
 *
 * @implSpec The changes in between two arbitrary actual versions MUST NOT be lost.
 * Thus, schema versions can only be removed from the beginning.
 * @implSpec Initial schema history MAY be registered without the first outdate versions
 * that could be cleaned up earlier.
 */
public class SchemaRegistry {
    /** Initial schema version. */
    public static final int INITIAL_SCHEMA_VERSION = -1;

    /** Latest actual schemas. */
    private final ConcurrentSkipListMap<Integer, SchemaDescriptor> history = new ConcurrentSkipListMap<>();

    /** Last registered version. */
    private volatile int lastVer;

    /**
     * Default constructor.
     */
    public SchemaRegistry() {
        lastVer = INITIAL_SCHEMA_VERSION;
    }

    /**
     * Constructor.
     *
     * @param history Schema history.
     */
    public SchemaRegistry(List<SchemaDescriptor> history) {
        if (history.isEmpty())
            lastVer = INITIAL_SCHEMA_VERSION;
        else {
            validateSchemaHistory(history);

            history.forEach(d -> this.history.put(d.version(), d));

            lastVer = history.get(history.size() - 1).version();
        }
    }

    /**
     * @param history Schema history.
     * @throws SchemaRegistryException If history is invalid.
     */
    private void validateSchemaHistory(List<SchemaDescriptor> history) {
        if (history.isEmpty())
            return;

        int prevVer = Objects.requireNonNull(history.get(0), "Schema descriptor can't be null.").version();

        assert prevVer > 0;

        for (int i = 1; i < history.size(); i++) {
            final SchemaDescriptor desc = Objects.requireNonNull(history.get(i), "Schema descriptor can't be null.");

            if (desc.version() != (++prevVer))
                throw new SchemaRegistryException("Unexpected schema version: expected=" + prevVer + ", actual=" + desc.version());
        }
    }

    /**
     * Gets schema descriptor for given version.
     *
     * @param ver Schema version to get descriptor for.
     * @return Schema descriptor.
     * @throws SchemaRegistryException If no schema found for given version.
     */
    public SchemaDescriptor schema(int ver) {
        final SchemaDescriptor desc = history.get(ver);

        if (desc != null)
            return desc;

        if (lastVer < ver || ver <= 0)
            throw new SchemaRegistryException("Incorrect schema version requested: " + ver);

        assert history.isEmpty() || ver < history.firstKey();

        throw new SchemaRegistryException("Outdated schema version requested: " + ver);
    }

    /**
     * Gets schema descriptor for the latest version if initialized.
     *
     * @return Schema descriptor if initialized, {@code null} otherwise.
     * @throws SchemaRegistryException If failed.
     */
    public @Nullable SchemaDescriptor schema() {
        final int lastVer0 = lastVer;

        final SchemaDescriptor desc = history.get(lastVer0);

        if (desc != null)
            return desc;

        if (lastVer0 == INITIAL_SCHEMA_VERSION)
            return null;

        throw new SchemaRegistryException("Failed to find last schema version: " + lastVer0);
    }

    /**
     * @return Last known schema version.
     */
    public int lastSchemaVersion() {
        return lastVer;
    }

    /**
     * Registers new schema.
     *
     * @param desc Schema descriptor.
     * @throws SchemaRegistrationConflictException If schema of provided version was already registered.
     * @throws SchemaRegistryException If schema of incorrect version provided.
     */
    public void registerSchema(SchemaDescriptor desc) {
        if (lastVer == INITIAL_SCHEMA_VERSION) {
            if (desc.version() != 1)
                throw new SchemaRegistryException("Try to register schema of wrong version: ver=" + desc.version() + ", lastVer=" + lastVer);
        }
        else if (desc.version() != lastVer + 1) {
            if (desc.version() > 0 && desc.version() <= lastVer)
                throw new SchemaRegistrationConflictException("Schema with given version has been already registered: " + desc.version());

            throw new SchemaRegistryException("Try to register schema of wrong version: ver=" + desc.version() + ", lastVer=" + lastVer);
        }

        history.put(desc.version(), desc);

        lastVer = desc.version();
    }

    /**
     * Cleanup history prior to given schema version.
     *
     * @param ver First actual schema version.
     * @throws SchemaRegistryException If incorrect schema version provided.
     */
    public void cleanupSchema(int ver) {
        if (ver > lastVer || ver <= 0)
            throw new SchemaRegistryException("Incorrect schema version to clean up to: " + ver);

        history.keySet().stream().takeWhile(k -> k < ver).forEach(history::remove);
    }
}
