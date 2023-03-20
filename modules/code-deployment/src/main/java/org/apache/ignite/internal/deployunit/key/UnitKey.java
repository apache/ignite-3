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

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Base64.Encoder;
import org.apache.ignite.lang.ByteArray;

/**
 * Helper for deployment units metastore keys generation.
 */
public final class UnitKey {
    private static final String DEPLOY_UNIT_PREFIX = "deploy-unit.";

    private static final String UNITS_PREFIX = DEPLOY_UNIT_PREFIX + "units.";

    private UnitKey() {

    }

    /**
     * Key to find all deployment units.
     *
     * @return Key in {@link ByteArray} format.
     */
    public static ByteArray allUnits() {
        return key(null, null);
    }

    /**
     * Key to find all deployment units with required id.
     *
     * @param id Required unit id.
     * @return Key in {@link ByteArray} format.
     */
    public static ByteArray withId(String id) {
        return key(id, null);
    }

    /**
     * Key for unit with required id and version. Only one unit should exist with this key.
     *  Version can be not {@code null} only in case when {@param id} is not {@code null}.
     *
     * @param id Required unit id.
     * @param version Required unit version.
     * @return Key in {@link ByteArray} format.
     */
    public static ByteArray key(String id, String version) {
        StringBuilder sb = new StringBuilder(UNITS_PREFIX);
        Encoder encoder = Base64.getEncoder();
        if (id != null) {
            sb.append(encoder.encodeToString(id.getBytes(StandardCharsets.UTF_8)));
            if (version != null) {
                sb.append(":");
                sb.append(encoder.encodeToString(version.getBytes(StandardCharsets.UTF_8)));
            }
        }
        return new ByteArray(sb.toString());
    }
}
