/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
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

package org.apache.ignite.configuration.internal;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.configuration.internal.property.DynamicProperty;
import org.apache.ignite.configuration.internal.property.Modifier;
import org.apache.ignite.configuration.internal.selector.BaseSelectors;
import org.apache.ignite.configuration.internal.validation.FieldValidator;

/**
 * This class represents configuration root or node.
 */
public abstract class DynamicConfiguration<T, INIT, CHANGE> implements Modifier<T, INIT, CHANGE> {
    /** Fully qualified name of the configuration. */
    protected final String qualifiedName;

    /** Configuration key. */
    protected final String key;

    /** Configuration prefix. */
    protected final String prefix;

    /** Configuration members (leafs and nodes). */
    protected final Map<String, Modifier<?, ?, ?>> members = new HashMap<>();

    /** Root configuration node. */
    protected final DynamicConfiguration<?, ?, ?> root;

    /** {@code true} if this is a member of {@link NamedListConfiguration}. */
    protected final boolean isNamed;

    /** Configurator that this configuration is attached to. */
    protected final Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator;

    /**
     * Constructor.
     * @param prefix Configuration prefix.
     * @param key Configuration key.
     * @param isNamed Is this a part of named configuration.
     * @param configurator Configurator that this object is attached to.
     * @param root Root configuration.
     */
    protected DynamicConfiguration(
        String prefix,
        String key,
        boolean isNamed,
        Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this.prefix = prefix;
        this.isNamed = isNamed;
        this.configurator = configurator;

        this.key = key;
        if (root == null)
            this.qualifiedName = key;
        else {
            if (isNamed)
                qualifiedName = String.format("%s[%s]", prefix, key);
            else
                qualifiedName = String.format("%s.%s", prefix, key);
        }

        this.root = root != null ? root : this;
    }

    /**
     * Add new configuration member.
     * @param member Configuration member (leaf or node).
     * @param <M> Type of member.
     */
    protected <M extends Modifier<?, ?, ?>> void add(M member) {
        members.put(member.key(), member);
    }

    /**
     * Add new configuration member with validators.
     * @param member Configuration member (leaf or node).
     * @param validators Validators for new member.
     * @param <PROP> Type of {@link DynamicProperty}.
     * @param <M> Type of member.
     */
    protected <PROP extends Serializable, M extends DynamicProperty<PROP>> void add(
        M member,
        List<FieldValidator<? super PROP, ? extends DynamicConfiguration<?, ?, ?>>> validators
    ) {
        members.put(member.key(), member);

        configurator.addValidations((Class<? extends DynamicConfiguration<?, ?, ?>>) getClass(), member.key(), validators);
    }

    /** {@inheritDoc} */
    @Override public void init(INIT init) {
        configurator.init(BaseSelectors.find(qualifiedName), init);
    }

    /** {@inheritDoc} */
    @Override public void change(CHANGE change) {
        configurator.set(BaseSelectors.find(qualifiedName), change);
    }

    /** {@inheritDoc} */
    @Override public String key() {
        return key;
    }

    /**
     * Create a deep copy of this DynamicConfiguration, but attaching it to another configuration root.
     * @param root New configuration root.
     * @return Copy of this configuration.
     */
    protected abstract DynamicConfiguration<T, INIT, CHANGE> copy(DynamicConfiguration<?, ?, ?> root);

    /**
     * Create a deep copy of this DynamicConfiguration, making it root configuration (so this method must be called
     * only on root configuration object).
     * @return Copy of this configuration.
     */
    protected final DynamicConfiguration<T, INIT, CHANGE> copy() {
        return copy(null);
    }

    /** {@inheritDoc} */
    @Override public void validate(DynamicConfiguration<?, ?, ?> oldRoot) {
        members.values().forEach(member -> member.validate(oldRoot));
    }

}
