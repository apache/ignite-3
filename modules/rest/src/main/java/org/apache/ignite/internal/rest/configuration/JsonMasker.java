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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * JSON masker.
 */
public class JsonMasker {

    private static final String MASKING_DIGIT = "*";

    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * Masks the given JSON string.
     *
     * @param json JSON string.
     * @param keysToMask Set of keys to mask. If empty, all keys will be masked.
     * @return Masked {@link JsonNode}.
     */
    public JsonNode mask(String json, Set<String> keysToMask) {
        Set<String> keysToMaskInLowerCase = keysToMask.stream()
                .map(it -> it.toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
        return traverseAndMask(stringToJsonNode(json).deepCopy(), keysToMaskInLowerCase);
    }

    private JsonNode traverseAndMask(JsonNode target, Set<String> keysToMask) {
        if (target.isTextual() && keysToMask.isEmpty()) {
            return new TextNode(maskString(target.textValue()));
        }

        if (target.isArray()) {
            for (int i = 0; i < target.size(); i++) {
                ((ArrayNode) target).set(i, traverseAndMask(target.get(i), keysToMask));
            }
        }

        if (target.isObject()) {
            ObjectNode targetObjectNode = (ObjectNode) target;
            target.fields().forEachRemaining(field -> {
                        if (keysToMask.isEmpty()) {
                            targetObjectNode.replace(field.getKey(), traverseAndMask(field.getValue(), emptySet()));
                        } else if (keysToMask.contains(field.getKey().toLowerCase(Locale.ROOT))) {
                            targetObjectNode.replace(field.getKey(), traverseAndMask(field.getValue(), emptySet()));
                        } else {
                            traverseAndMask(field.getValue(), keysToMask);
                        }
                    }
            );
        }

        return target;
    }

    private String maskString(String str) {
        return MASKING_DIGIT.repeat(str.length());
    }

    private JsonNode stringToJsonNode(String str) {
        JsonFactory factory = mapper.getFactory();
        try {
            JsonParser parser = factory.createParser(str);
            return mapper.readTree(parser);
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid JSON string: " + str, e);
        }
    }
}
