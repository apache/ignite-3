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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.deployment.version.VersionParseException;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.UnitStatus;
import org.jetbrains.annotations.Nullable;

/**
 * Serializer for {@link UnitStatus}.
 */
public final class SerializeUtils {
    private static final String SEPARATOR = ";";

    private static final String LIST_SEPARATOR = ":";

    /**
     * Constructor.
     */
    private SerializeUtils() {

    }

    /**
     * Serialize unit meta.
     *
     * @param args Unit status.
     * @return Serialized unit status.
     */
    static byte[] serialize(Object... args) {
        StringBuilder sb = new StringBuilder();

        for (Object arg : args) {
            if (arg == null) {
                sb.append(SEPARATOR);
                continue;
            }

            if (arg instanceof Collection) {
                appendWithEncoding(sb, (Collection<String>) arg);
            } else {
                appendWithEncoding(sb, arg.toString());
            }
        }

        return sb.toString().getBytes(UTF_8);
    }

    /**
     * Deserialize byte array to unit status.
     *
     * @param bytes Byte array.
     * @return Unit status.
     */
    static String[] deserialize(byte[] bytes) {
        String s = new String(bytes, UTF_8);

        return s.split(SEPARATOR, -1);
    }

    static Set<String> decodeAsSet(String s) {
        if (s == null || s.isBlank()) {
            return Collections.emptySet();
        }
        String[] split = s.split(LIST_SEPARATOR, -1);
        return Arrays.stream(split).map(SerializeUtils::decode).collect(Collectors.toSet());
    }

    private static void appendWithEncoding(StringBuilder sb, String content) {
        sb.append(encode(content)).append(SEPARATOR);
    }

    private static void appendWithEncoding(StringBuilder sb, Collection<String> content) {
        String list = content.stream()
                .map(SerializeUtils::encode)
                .collect(Collectors.joining(LIST_SEPARATOR));
        sb.append(list).append(SEPARATOR);
    }

    private static String encode(String s) {
        return new String(Base64.getEncoder().encode(s.getBytes(UTF_8)), UTF_8);
    }

    static String decode(String s) {
        return new String(Base64.getDecoder().decode(s), UTF_8);
    }

    static boolean checkElement(String[] arr, int index) {
        return arr.length > index && arr[index] != null && !arr[index].isBlank();
    }

    @Nullable
    static Version deserializeVersion(String[] values, int index) {
        try {
            return checkElement(values, index) ? Version.parseVersion(decode(values[index])) : null;
        } catch (VersionParseException e) {
            return null;
        }
    }

    @Nullable
    static DeploymentStatus deserializeStatus(String[] values, int index) {
        try {
            return checkElement(values, index) ? DeploymentStatus.valueOf(decode(values[index])) : null;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    @Nullable
    static UUID deserializeUuid(String[] values, int index) {
        try {
            return checkElement(values, index) ? UUID.fromString(decode(values[index])) : null;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
