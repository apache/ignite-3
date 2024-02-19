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

package org.apache.ignite.internal.index;

import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
import org.apache.ignite.internal.tostring.S;

/** {@link ChangeIndexStatusTask} ID. */
class ChangeIndexStatusTaskId {
    private final int indexId;

    private final CatalogIndexStatus status;

    ChangeIndexStatusTaskId(int indexId, CatalogIndexStatus status) {
        this.indexId = indexId;
        this.status = status;
    }

    ChangeIndexStatusTaskId(CatalogIndexDescriptor indexDescriptor) {
        this.indexId = indexDescriptor.id();
        this.status = indexDescriptor.status();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChangeIndexStatusTaskId that = (ChangeIndexStatusTaskId) o;

        return indexId == that.indexId && status == that.status;
    }

    @Override
    public int hashCode() {
        int result = indexId;
        result = 31 * result + status.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return S.toString(ChangeIndexStatusTaskId.class, this);
    }
}
