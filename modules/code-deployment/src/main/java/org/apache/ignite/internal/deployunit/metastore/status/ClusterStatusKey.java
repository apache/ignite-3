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

import static org.apache.ignite.internal.deployunit.metastore.status.UnitKey.DEPLOY_UNIT_PREFIX;

import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.lang.ByteArray;

/**
 * Deployment unit cluster status store key.
 */
public class ClusterStatusKey {
    private static final String UNITS_PREFIX = DEPLOY_UNIT_PREFIX + "units.";

    private final String id;

    private final Version version;

    /**
     * Constructor.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     */
    private ClusterStatusKey(String id, Version version) {
        this.id = id;
        this.version = version;
    }

    /**
     * Serialize key instance to {@link ByteArray}.
     *
     * @return {@link ByteArray} instance with serialized content.
     */
    public ByteArray toByteArray() {
        return UnitKey.toByteArray(UNITS_PREFIX, id, version == null ? null : version.render());
    }

    /**
     * Deserialize key instance {@link ClusterStatusKey} from byte array.
     *
     * @param key Serialized key in byte array.
     * @return Deserialized deployment unit cluster key.
     */
    public static ClusterStatusKey fromBytes(byte[] key) {
        String[] parse = UnitKey.fromBytes(UNITS_PREFIX, key);
        int length = parse.length;
        String id = length > 0 ? parse[0] : null;
        Version version = length > 1 ? Version.parseVersion(parse[1]) : null;

        return builder().id(id).version(version).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ClusterStatusKey that = (ClusterStatusKey) o;

        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        return version != null ? version.equals(that.version) : that.version == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        return result;
    }

    public static ClusterStatusKeyBuilder builder() {
        return new ClusterStatusKeyBuilder();
    }

    /**
     * Builder for {@link ClusterStatusKey}.
     */
    public static class ClusterStatusKeyBuilder {
        private String id;

        private Version version;

        public ClusterStatusKeyBuilder id(String id) {
            this.id = id;
            return this;
        }

        public ClusterStatusKeyBuilder version(Version version) {
            this.version = version;
            return this;
        }

        public ClusterStatusKey build() {
            return new ClusterStatusKey(id, version);
        }
    }
}
