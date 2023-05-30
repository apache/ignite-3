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

package org.apache.ignite.internal.deployunit.metastore.key;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Encoder;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.lang.ByteArray;

/**
 * Helper for deployment units metastore keys generation.
 */
public final class UnitKey {
    private static final String DEPLOY_UNIT_PREFIX = "deploy-unit.";

    private static final String UNITS_PREFIX = DEPLOY_UNIT_PREFIX + "units.";

    private static final String NODES_PREFIX = DEPLOY_UNIT_PREFIX + "nodes.";

    private static final String DELIMITER = ":";

    private UnitKey() {

    }

    /**
     * Key to find all deployment units.
     *
     * @return Key in {@link ByteArray} format.
     */
    public static ByteArray allUnits() {
        return clusterStatusKey(null, null);
    }

    /**
     * Key to find all deployment units with required id.
     *
     * @param id Required unit id.
     * @return Key in {@link ByteArray} format.
     */
    public static ByteArray withId(String id) {
        return clusterStatusKey(id, null);
    }

    /**
     * Returns prefix for all nodes entries.
     *
     * @return Prefix for all nodes entries.
     */
    public static ByteArray nodes() {
        return nodeStatusKey(null, null, null);
    }

    /**
     * Returns prefix for all nodes entries for deployment units with {@param id}.
     *
     * @param id Deployment unit identifier.
     * @return Prefix for all nodes entries for deployment units with {@param id}.
     */
    public static ByteArray nodes(String id) {
        return nodeStatusKey(id, null, null);
    }

    /**
     * Returns prefix for all nodes entries for deployment units with {@param id} and {@param version}.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Prefix for all nodes entries for deployment units with {@param id} and {@param version}.
     */
    public static ByteArray nodes(String id, Version version) {
        return nodeStatusKey(id, version, null);
    }

    /**
     * Trying to extract node consistent identifier from {@param key}.
     *
     * @return Node consistent identifier from {@param key}.
     * @throws IllegalArgumentException in case when node consistent identifier is not presented in {@param key}.
     */
    public static String extractNodeId(byte[] key) {
        String keyStr = new String(key, StandardCharsets.UTF_8);
        if (!keyStr.startsWith(NODES_PREFIX)) {
            throw new IllegalArgumentException("Provided key doesn't contain node information.");
        }

        String keyContent = keyStr.substring(NODES_PREFIX.length());
        String[] split = keyContent.split(DELIMITER);
        return new String(Base64.getDecoder().decode(split[split.length - 1]), StandardCharsets.UTF_8);
    }

    /**
     * Key for cluster status with required id and version. Only one unit should exist with this key.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Key in {@link ByteArray} format.
     */
    public static ByteArray clusterStatusKey(String id, Version version) {
        return key(UNITS_PREFIX, id, version == null ? null : version.render());
    }

    /**
     * Key for node status with required id and version. Only one unit should exist with this key.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param nodeId Node consistent identifier.
     * @return Key in {@link ByteArray} format.
     */
    public static ByteArray nodeStatusKey(String id, Version version, String nodeId) {
        return key(NODES_PREFIX, id, version == null ? null : version.render(), nodeId);
    }

    private static ByteArray key(String prefix, String... args) {
        Encoder encoder = Base64.getEncoder();
        String collect = Arrays.stream(args).filter(Objects::nonNull)
                .map(arg -> encoder.encodeToString(arg.getBytes(StandardCharsets.UTF_8)))
                .collect(Collectors.joining(DELIMITER));

        return new ByteArray(prefix + collect);
    }
}
