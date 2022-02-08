/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.join;

import java.util.Objects;

/**
 * Data class containing consistent IDs of leaders of the Meta Storage and the CMG.
 */
public class Leaders {
    private final String metaStorageLeader;

    private final String cmgLeader;

    public Leaders(String metaStorageLeader, String cmgLeader) {
        this.metaStorageLeader = metaStorageLeader;
        this.cmgLeader = cmgLeader;
    }

    public String metaStorageLeader() {
        return metaStorageLeader;
    }

    public String cmgLeader() {
        return cmgLeader;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Leaders leaders = (Leaders) o;
        return metaStorageLeader.equals(leaders.metaStorageLeader) && cmgLeader.equals(leaders.cmgLeader);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metaStorageLeader, cmgLeader);
    }
}
