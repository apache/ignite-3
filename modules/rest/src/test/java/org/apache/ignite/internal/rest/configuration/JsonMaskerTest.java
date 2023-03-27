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

package org.apache.ignite.internal.rest.configuration;

import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Set;
import org.junit.jupiter.api.Test;

class JsonMaskerTest {

    private final Set<String> keysToMask = Set.of("credentials");
    private final JsonMasker jsonMasker = new JsonMasker();

    @Test
    void notingToMask() {
        String json = "{\"name\": \"John Doe\", \"age\": 30}";
        String masked = jsonMasker.mask(json, keysToMask).toString();
        assertEquals("{\"name\":\"John Doe\",\"age\":30}", masked);
    }

    @Test
    void maskAll() {
        String json = "{\"name\": \"John Doe\", \"age\": 30}";
        String masked = jsonMasker.mask(json, emptySet()).toString();
        assertEquals("{\"name\":\"********\",\"age\":30}", masked);
    }

    @Test
    void maskField() {
        String json = "{\"name\": \"John Doe\", \"age\": 30, \"credentials\": \"admin\"}";
        String masked = jsonMasker.mask(json, keysToMask).toString();
        assertEquals("{\"name\":\"John Doe\",\"age\":30,\"credentials\":\"*****\"}", masked);
    }

    @Test
    void maskNestedArray() {
        String json = "{\"name\": \"John Doe\", \"age\": 30, \"credentials\": [\"admin1\", \"admin2\"]}";
        String masked = jsonMasker.mask(json, keysToMask).toString();
        assertEquals("{\"name\":\"John Doe\",\"age\":30,\"credentials\":[\"******\",\"******\"]}", masked);
    }

    @Test
    void maskNestedObject() {
        String json = "{\n"
                + "  \"name\": \"John Doe\",\n"
                + "  \"age\": 30,\n"
                + "  \"credentials\": {\n"
                + "    \"basic\": {\n"
                + "      \"user\": \"admin\",\n"
                + "      \"password\": \"admin\"\n"
                + "    }\n"
                + "  }\n"
                + "}";
        String masked = jsonMasker.mask(json, keysToMask).toString();
        assertEquals(
                "{\"name\":\"John Doe\",\"age\":30,\"credentials\":{\"basic\":{\"user\":\"*****\",\"password\":\"*****\"}}}",
                masked
        );
    }

    @Test
    void booleanNotMasked() {
        String json = "{\"name\": \"John Doe\", \"age\": 30, \"credentials\": true}";
        String masked = jsonMasker.mask(json, keysToMask).toString();
        assertEquals("{\"name\":\"John Doe\",\"age\":30,\"credentials\":true}", masked);
    }

    @Test
    void numberNotMasked() {
        String json = "{\"name\": \"John Doe\", \"age\": 30, \"credentials\": 123}";
        String masked = jsonMasker.mask(json, keysToMask).toString();
        assertEquals("{\"name\":\"John Doe\",\"age\":30,\"credentials\":123}", masked);
    }

    @Test
    void fieldInUpperCase() {
        String json = "{\"name\": \"John Doe\", \"age\": 30, \"CREDENTIALS\": \"admin\"}";
        String masked = jsonMasker.mask(json, keysToMask).toString();
        assertEquals("{\"name\":\"John Doe\",\"age\":30,\"CREDENTIALS\":\"*****\"}", masked);
    }
}
