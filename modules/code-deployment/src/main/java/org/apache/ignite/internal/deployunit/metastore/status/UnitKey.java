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

package org.apache.ignite.internal.deployunit.metastore.status;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.internal.lang.ByteArray;

/**
 * Helper for deployment units metastore keys generation.
 */
public final class UnitKey {
    public static final String DEPLOY_UNIT_PREFIX = "deploy-unit.";

    private static final String DELIMITER = ":";

    private UnitKey() {
    }

    static String[] fromBytes(String prefix, byte[] key) {
        String s = new String(key, StandardCharsets.UTF_8);

        if (!s.startsWith(prefix)) {
            return new String[0];
        }

        String content = s.substring(prefix.length());
        if (content.isEmpty()) {
            return new String[0];
        }
        return Arrays.stream(content.split(DELIMITER))
                .map(e -> new String(Base64.getDecoder().decode(e), StandardCharsets.UTF_8))
                .toArray(String[]::new);
    }

    static ByteArray toByteArray(String prefix, String id, String... args) {
        Encoder encoder = Base64.getEncoder();

        // Always add a delimiter if id is present so that the lookup on id will happen on exact match
        String idStr = id != null ? encoder.encodeToString(id.getBytes(StandardCharsets.UTF_8)) + DELIMITER : "";

        String argsStr = Arrays.stream(args).filter(Objects::nonNull)
                .map(arg -> encoder.encodeToString(arg.getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.joining(DELIMITER));

        return new ByteArray(prefix + idStr + argsStr);
    }
}
