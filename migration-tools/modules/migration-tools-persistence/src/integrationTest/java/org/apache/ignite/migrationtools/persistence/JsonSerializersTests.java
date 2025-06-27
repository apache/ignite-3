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

package org.apache.ignite.migrationtools.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.migrationtools.persistence.mappers.AbstractSchemaColumnsProcessor;
import org.apache.ignite.migrationtools.tests.clusters.FullSampleCluster;
import org.apache.ignite.migrationtools.tests.e2e.impl.MyOrganizationsCacheTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JsonSerializersTests. */
@DisabledIfSystemProperty(
        named = "tests.containers.support",
        matches = "false",
        disabledReason = "Lack of support in TeamCity for testcontainers")
@ExtendWith(FullSampleCluster.class)
public class JsonSerializersTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonSerializersTests.class);

    @ExtendWith(BasePersistentTestContext.class)
    private List<MigrationKernalContext> nodeContexts;

    private List<MigrationKernalContext> firstNode;

    private final ObjectMapper mapper = AbstractSchemaColumnsProcessor.createMapper();

    @BeforeEach
    void startContexts() throws IgniteCheckedException {
        firstNode = List.of(nodeContexts.get(0));
        for (MigrationKernalContext ctx : firstNode) {
            ctx.start();
        }
        // Stop is done automatically.
    }

    @Test
    void loadBinaryCacheObjectAndSerialize() throws IgniteCheckedException, JsonProcessingException {
        var cacheTest = new MyOrganizationsCacheTest();
        List<String> expectedFieldNames = List.of("addr", "type", "lastUpdated");

        MutableObject<Throwable> errorRef = new MutableObject<>(null);
        Map<Integer, String> jsonValues = new HashMap<>();
        Ignite2PersistentCacheTools.publishCacheCursor(firstNode, cacheTest.cacheConfiguration().getName(), (publisher, partId) -> {
            if (jsonValues.size() >= 10 || errorRef.getValue() != null) {
                return Optional.empty();
            }

            CompletableFuture<Void> f = new CompletableFuture<>();
            publisher.subscribe(new Flow.Subscriber<>() {
                private Flow.Subscription subscription;

                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    this.subscription = subscription;
                    this.subscription.request(1);
                }

                @Override
                public void onNext(Map.Entry<Object, Object> item) {
                    var tmp = new AbstractSchemaColumnsProcessor.WrapperClass((BinaryObjectImpl) item.getValue(), expectedFieldNames);

                    try {
                        String jsonStr = mapper.writeValueAsString(tmp);
                        jsonValues.put(Math.toIntExact((Long) item.getKey()), jsonStr);
                        this.subscription.request(1);
                    } catch (JsonProcessingException e) {
                        onError(e);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    LOGGER.error("Error on subscriber", throwable);
                    errorRef.setValue(throwable);
                    subscription.cancel();
                    f.complete(null);
                }

                @Override
                public void onComplete() {
                    f.complete(null);
                }
            });

            return Optional.of(f);
        });

        assertThat(errorRef.getValue()).isNull();

        var simpleMapper = new ObjectMapper()
                .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        for (var e : jsonValues.entrySet()) {
            String jsonStr = e.getValue();
            var ex = cacheTest.supplyExample(e.getKey()).getValue();

            JsonNode node = simpleMapper.readTree(jsonStr);

            assertThat(IteratorUtils.toList(node.fieldNames()))
                    .containsExactlyInAnyOrderElementsOf(expectedFieldNames);

            var actual = simpleMapper.readValue(jsonStr, Organization.class);

            assertThat(actual)
                    .usingRecursiveComparison()
                    .ignoringFields("id", "name")
                    .isEqualTo(ex);
        }
    }
}
