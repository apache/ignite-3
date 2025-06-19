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

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Node that represents a configuration value.
 */
class ValueNode extends ConfigNode {
    private final Map<String, String> additionalAttributes;

    ValueNode(String name, String type, ConfigNode parent, Map<String, String> additionalAttributes) {
        super(name, type, parent);
        this.additionalAttributes = additionalAttributes;
    }

    public Map<String, String> additionalAttributes() {
        return additionalAttributes;
    }

    @Override
    public boolean isValue() {
        return true;
    }

    @Override
    protected String toString0() {
        return super.toString0() + ((additionalAttributes.isEmpty()) ? ""
                : additionalAttributes.entrySet().stream()
                        .map((e) -> e.getKey() + "=" + e.getValue())
                        .collect(Collectors.joining(", ", ", ", "")));
    }
}
