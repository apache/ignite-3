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

import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration inner node.
 */
public class InnerNode extends ConfigNode {
    private final List<ConfigNode> children;

    InnerNode(String name, String type, @Nullable ConfigNode parent, List<ConfigNode> children) {
        super(name, type, parent);
        this.children = Collections.unmodifiableList(children);
    }

    public List<ConfigNode> children() {
        return children;
    }

    @Override
    public void accept(ConfigShuttle visitor) {
        super.accept(visitor);

        for (ConfigNode child : children) {
            child.accept(visitor);
        }
    }

    @Override
    protected String toString0() {
        return super.toString0() + ", childs=" + children.size();
    }
}
