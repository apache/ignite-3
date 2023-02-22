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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.deployment.version.Version;

/**
 * Serializer for {@link UnitMeta}.
 */
public class UnitMetaSerializer {
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

        sb.append(meta.getId()).append(SEPARATOR)
                .append(meta.getVersion().render()).append(SEPARATOR)
                .append(meta.getUnitName()).append(SEPARATOR);

        for (String id : meta.getConsistentIdLocation()) {
            sb.append(id).append(LIST_SEPARATOR);
        }
        sb.append(SEPARATOR);

        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Deserialize byte array to unit meta.
     *
     * @param bytes Byte array.
     * @return Unit meta.
     */
    public static UnitMeta deserialize(byte[] bytes) {
        String s = new String(bytes, StandardCharsets.UTF_8);

        String[] split = s.split(SEPARATOR);

        String id = split[0];

        String version = split[1];

        String unitName = split[2];

        String consistentIds = split.length > 3 ? split[3] : "";

        String[] split1 = consistentIds.isEmpty() ? new String[0] : consistentIds.split(LIST_SEPARATOR);
        List<String> ids = split1.length == 0 ? Collections.emptyList() : Arrays.asList(split1);

        return new UnitMeta(id, Version.parseVersion(version), unitName, ids);
    }
}
