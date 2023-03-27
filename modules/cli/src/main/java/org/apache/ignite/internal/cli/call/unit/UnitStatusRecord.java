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

package org.apache.ignite.internal.cli.call.unit;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Unit status record. */
public class UnitStatusRecord {
    private final String id;
    private final Map<String, List<String>> versionToConsistentIds;

    UnitStatusRecord(String id, Map<String, List<String>> versionToConsistentIds) {
        this.id = id;
        this.versionToConsistentIds = versionToConsistentIds;
    }

    public String id() {
        return id;
    }

    public Map<String, List<String>> versionToConsistentIds() {
        return versionToConsistentIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnitStatusRecord that = (UnitStatusRecord) o;
        return Objects.equals(id, that.id) && Objects.equals(versionToConsistentIds, that.versionToConsistentIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, versionToConsistentIds);
    }

    @Override
    public String toString() {
        return "UnitStatusRecord{"
                + "id='" + id + '\''
                + ", versionToConsistentIds=" + versionToConsistentIds
                + '}';
    }
}
