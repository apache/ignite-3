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

package org.apache.ignite.internal.rest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micronaut.context.annotation.Property;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.time.Instant;
import org.junit.jupiter.api.Test;

@MicronautTest
@Property(name = "micronaut.security.enabled", value = "false")
@Property(name = "jackson.deserialization.use_big_decimal_for_floats", value = "true")
class InstantDeserializationTest {
    // Specific value which will be truncated if deserialized as the double
    private static final Instant INSTANT = Instant.ofEpochSecond(1730977056L, 232784600);

    @Inject
    @Client("/time")
    HttpClient client;

    @Test
    void micronautDeserialization() {
        // The TestController will deserialize the instant and return a string representation.
        String response = client.toBlocking().retrieve(HttpRequest.POST("/", new TimeDto(INSTANT)));

        assertThat(response, is(INSTANT.toString()));
    }

    @Test
    void jacksonDeserialization() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        mapper.registerModule(new JavaTimeModule());

        String serializedInstant = mapper.writeValueAsString(INSTANT);
        assertThat(serializedInstant, is("1730977056.232784600"));

        JsonNode jsonNode = mapper.readTree(serializedInstant);

        Instant deserializedInstant = mapper.treeToValue(jsonNode, Instant.class);

        assertThat(deserializedInstant.getEpochSecond(), is(INSTANT.getEpochSecond()));
        assertThat(deserializedInstant.getNano(), is(INSTANT.getNano()));
    }
}
