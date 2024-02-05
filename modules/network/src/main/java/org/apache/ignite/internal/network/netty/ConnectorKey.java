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

package org.apache.ignite.internal.network.netty;

import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.tostring.S;

/**
 * Composite key object for connectors.
 *
 * @param <T> identifier type.
 */
public class ConnectorKey<T> {
    private final T id;

    private final ChannelType type;

    /**
     * Constructor.
     *
     * @param id Connector identifier.
     * @param type Channel type.
     */
    public ConnectorKey(T id, ChannelType type) {
        this.id = id;
        this.type = type;
    }

    public T id() {
        return id;
    }

    public ChannelType type() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ConnectorKey<?> that = (ConnectorKey<?>) o;

        return (id != null ? id.equals(that.id) : that.id == null)
                && (type != null ? type.equals(that.type) : that.type == null);
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return S.toString(ConnectorKey.class, this);
    }
}
