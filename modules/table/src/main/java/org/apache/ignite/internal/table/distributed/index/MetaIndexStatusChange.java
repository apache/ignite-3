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

package org.apache.ignite.internal.table.distributed.index;

import java.io.Serializable;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.tostring.S;

/** Information about {@link MetaIndexStatus index status} changes. */
public class MetaIndexStatusChange implements Serializable {
    private static final long serialVersionUID = -2837971110813775936L;

    private final int catalogVersion;

    private final long activationTs;

    /** Constructor. */
    MetaIndexStatusChange(int catalogVersion, long activationTs) {
        this.catalogVersion = catalogVersion;
        this.activationTs = activationTs;
    }

    /** Returns the catalog version in which the {@link MetaIndexStatus status} appeared. */
    public int catalogVersion() {
        return catalogVersion;
    }

    /**
     * Returns the activation time of the {@link #catalogVersion()}.
     *
     * @see Catalog#time()
     */
    public long activationTimestamp() {
        return activationTs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MetaIndexStatusChange that = (MetaIndexStatusChange) o;

        return catalogVersion == that.catalogVersion && activationTs == that.activationTs;
    }

    @Override
    public int hashCode() {
        int result = catalogVersion;
        result = 31 * result + (int) (activationTs ^ (activationTs >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
