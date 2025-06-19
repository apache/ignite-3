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

package org.apache.ignite.internal.configuration.compatibility.framework;

import java.util.Objects;

/**
 * Base class for configuration nodes in the configuration tree.
 */
public abstract class ConfigNode {
    protected final String name;
    protected final String type;
    protected final ConfigNode parent;

    ConfigNode(String name, String type, ConfigNode parent) {
        this.name = name;
        this.type = type;
        this.parent = parent;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public boolean isRoot() {
        return parent == null;
    }

    public boolean isValue() {
        return false;
    }

    public final String path() {
        return parent == null ? name : parent.path() + '.' + name;
    }

    protected String toString0() {
        return "isRoot=" + isRoot() + ", isValue=" + isValue() + ", type=" + type;
    }

    public void accept(ConfigShuttle visitor) {
        visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        return toString().equals(o.toString());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type);
    }

    @Override
    public final String toString() {
        return path() + ": [" + toString0() + ']';
    }
}
