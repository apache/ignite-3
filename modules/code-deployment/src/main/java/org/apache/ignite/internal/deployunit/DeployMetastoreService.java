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

package org.apache.ignite.internal.deployunit;

import static org.apache.ignite.internal.deployunit.key.UnitKey.allUnits;
import static org.apache.ignite.internal.deployunit.key.UnitKey.key;
import static org.apache.ignite.internal.deployunit.key.UnitKey.withId;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Consumer;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.key.UnitMetaSerializer;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition;
import org.apache.ignite.lang.ByteArray;

/**
 * Service for metastore access of deployment units.
 */
public class DeployMetastoreService {
    /**
     * Meta storage.
     */
    private final MetaStorageManager metaStorage;

    public DeployMetastoreService(MetaStorageManager metaStorage) {
        this.metaStorage = metaStorage;
    }

    /**
     * Update deployment unit meta.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param transformer Deployment unit meta transformer.
     * @return Future with update result.
     */
    public CompletableFuture<Boolean> updateMeta(String id, Version version, Consumer<UnitMeta> transformer) {
        ByteArray key = key(id, version);
        return metaStorage.get(key)
                .thenCompose(e -> {
                    UnitMeta prev = UnitMetaSerializer.deserialize(e.value());

                    transformer.accept(prev);

                    SimpleCondition eq = revision(key).le(e.revision());
                    return metaStorage.invoke(
                            eq,
                            put(key, UnitMetaSerializer.serialize(prev)),
                            noop());
                });
    }

    /**
     * Put deployment unit meta if not exists.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param meta Deployment unit meta.
     * @return Future with put result.
     */
    public CompletableFuture<Boolean> putIfNotExist(String id, Version version, UnitMeta meta) {
        ByteArray key = key(id, version);
        Operation put = put(key, UnitMetaSerializer.serialize(meta));
        return metaStorage.invoke(notExists(key), put, noop());
    }

    /**
     * Remove deployment unit meta if exist.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     */
    public void removeIfExist(String id, Version version) {
        ByteArray key = key(id, version);
        metaStorage.invoke(exists(key), remove(key), noop());
    }

    /**
     * Returns {@link Publisher} instance with all deployment unit metas.
     *
     * @return {@link Publisher} instance with all deployment unit metas.
     */
    public Publisher<Entry> getAll() {
        return metaStorage.prefix(allUnits());
    }

    /**
     * Returns {@link Publisher} instance with all deployment unit metas with provided id.
     *
     * @param id Deployment unit identifier.
     * @return {@link Publisher} instance with all deployment unit metas with provided id.
     */
    public Publisher<Entry> getAllWithId(String id) {
        return metaStorage.prefix(withId(id));
    }

}
