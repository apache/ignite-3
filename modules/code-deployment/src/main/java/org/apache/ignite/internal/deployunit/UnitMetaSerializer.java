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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;
import java.util.List;
import org.apache.ignite.deployment.version.Version;

/**
 * Serializer for {@link UnitMeta}.
 */
public class UnitMetaSerializer {
    private static final String SEPARATOR = ";";

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

        appendWithEncoding(sb, meta.getId());
        appendWithEncoding(sb, meta.getVersion().render());
        appendWithEncoding(sb, meta.getUnitName());

        for (String id : meta.getConsistentIdLocation()) {
            appendWithEncoding(sb, id);
        }

        return sb.toString().getBytes(UTF_8);
    }

    private static void appendWithEncoding(StringBuilder sb, String content) {
        Encoder encoder = Base64.getEncoder();
        sb.append(new String(encoder.encode(content.getBytes(UTF_8)), UTF_8)).append(SEPARATOR);
    }

    /**
     * Deserialize byte array to unit meta.
     *
     * @param bytes Byte array.
     * @return Unit meta.
     */
    public static UnitMeta deserialize(byte[] bytes) {
        String s = new String(bytes, UTF_8);

        String[] split = s.split(SEPARATOR);

        Decoder decoder = Base64.getDecoder();

        String id = new String(decoder.decode(split[0]), UTF_8);

        String version = new String(decoder.decode(split[1]), UTF_8);

        String unitName = new String(decoder.decode(split[2]), UTF_8);

        List<String> ids = new ArrayList<>();
        for (int i = 3; i < split.length; i++) {
            ids.add(new String(decoder.decode(split[i]), UTF_8));
        }

        return new UnitMeta(id, Version.parseVersion(version), unitName, ids);
    }
}
