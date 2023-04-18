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

package org.apache.ignite.internal.deployunit.key;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.deployunit.UnitMeta;
import org.apache.ignite.internal.deployunit.version.Version;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;

/**
 * Serializer for {@link UnitMeta}.
 */
public final class UnitMetaSerializer {
    private static final String SEPARATOR = ";";

    private static final String LIST_SEPARATOR = ":";

    /**
     * Constructor.
     */
    private UnitMetaSerializer() {

    }

    /**
     * Serialize unit meta.
     *
     * @param meta Unit meta.
     * @return Serialized unit meta.
     */
    public static byte[] serialize(UnitMeta meta) {
        StringBuilder sb = new StringBuilder();

        appendWithEncoding(sb, meta.id());
        appendWithEncoding(sb, meta.version().render());
        appendWithEncoding(sb, meta.fileNames());
        appendWithEncoding(sb, meta.status().name());
        appendWithEncoding(sb, meta.consistentIdLocation());

        return sb.toString().getBytes(UTF_8);
    }

    /**
     * Deserialize byte array to unit meta.
     *
     * @param bytes Byte array.
     * @return Unit meta.
     */
    public static UnitMeta deserialize(byte[] bytes) {
        String s = new String(bytes, UTF_8);
        String[] split = s.split(SEPARATOR, -1);

        String id = decode(split[0]);
        String version = decode(split[1]);
        List<String> fileNames = deserializeList(split[2]);

        DeploymentStatus status = DeploymentStatus.valueOf(decode(split[3]));

        List<String> ids = deserializeList(split[4]);

        return new UnitMeta(id, Version.parseVersion(version), fileNames, status, ids);
    }

    private static void appendWithEncoding(StringBuilder sb, String content) {
        sb.append(encode(content)).append(SEPARATOR);
    }

    private static void appendWithEncoding(StringBuilder sb, List<String> content) {
        String list = content.stream()
                .map(UnitMetaSerializer::encode)
                .collect(Collectors.joining(LIST_SEPARATOR));
        sb.append(list).append(SEPARATOR);
    }

    private static List<String> deserializeList(String data) {
        if (data.isEmpty()) {
            return Collections.emptyList();
        }

        return Arrays.stream(data.split(LIST_SEPARATOR))
                .map(UnitMetaSerializer::decode)
                .collect(Collectors.toList());
    }

    private static String encode(String s) {
        return new String(Base64.getEncoder().encode(s.getBytes(UTF_8)), UTF_8);
    }

    private static String decode(String s) {
        return new String(Base64.getDecoder().decode(s), UTF_8);
    }
}
