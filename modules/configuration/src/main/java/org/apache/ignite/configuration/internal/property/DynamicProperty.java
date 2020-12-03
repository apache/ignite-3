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

package org.apache.ignite.configuration.internal.property;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.configuration.internal.Configurator;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.selector.BaseSelectors;
import org.apache.ignite.configuration.internal.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.internal.validation.FieldValidator;
import org.apache.ignite.configuration.internal.validation.MemberKey;

/**
 * Holder for property value. Expected to be used with numbers, strings and other immutable objects, e.g. IP addresses.
 */
public class DynamicProperty<T extends Serializable> implements Modifier<T, T, T> {
    /** Name of property. */
    private final String name;

    /** Member key. */
    private final MemberKey memberKey;

    /** Full name with prefix. */
    private final String qualifiedName;

    /** Property value. */
    protected volatile T val;

    /** Listeners of property update. */
    private final List<PropertyListener<T, T, T>> updateListeners = new ArrayList<>();

    /** Configurator that this configuration is attached to. */
    protected final Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator;

    protected final DynamicConfiguration<?, ?, ?> root;

    /**
     * Constructor.
     * @param prefix Property prefix.
     * @param name Property name.
     * @param memberKey Property member key.
     * @param configurator Configurator to attach to.
     * @param root Configuration root.
     */
    public DynamicProperty(
        String prefix,
        String name,
        MemberKey memberKey,
        Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this(prefix, name, memberKey, null, configurator, root);
    }

    /**
     * Constructor.
     * @param prefix Property prefix.
     * @param name Property name.
     * @param memberKey Property member key.
     * @param defaultValue Default value for the property.
     * @param configurator Configurator to attach to.
     * @param root Configuration root.
     */
    public DynamicProperty(
        String prefix,
        String name,
        MemberKey memberKey,
        T defaultValue,
        Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this(defaultValue, name, memberKey, String.format("%s.%s", prefix, name), configurator, root);
    }

    /**
     * Copy constructor.
     * @param base Property to copy from.
     * @param root Configuration root.
     */
    private DynamicProperty(
        DynamicProperty<T> base,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this(base.val, base.name, base.memberKey, base.qualifiedName, base.configurator, root);
    }

    /**
     * Constructor.
     * @param value Property value.
     * @param name Property name.
     * @param memberKey Member key.
     * @param qualifiedName Fully qualified name of the property.
     * @param configurator Configurator.
     * @param root Configuration root.
     */
    private DynamicProperty(
        T value,
        String name,
        MemberKey memberKey,
        String qualifiedName,
        Configurator<? extends DynamicConfiguration<?, ?, ?>> configurator,
        DynamicConfiguration<?, ?, ?> root
    ) {
        this.name = name;
        this.memberKey = memberKey;
        this.qualifiedName = qualifiedName;
        this.val = value;
        this.configurator = configurator;
        this.root = root;
    }

    /**
     * Add change listener to this property.
     * @param listener Property change listener.
     */
    public void addListener(PropertyListener<T, T, T> listener) {
        updateListeners.add(listener);
    }

    /**
     * Get value of this property.
     * @return Value of this property.
     */
    public T value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public T toView() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public void change(T object) throws ConfigurationValidationException {
        configurator.set(BaseSelectors.find(qualifiedName), object);
    }

    /** {@inheritDoc} */
    @Override public void init(T object) throws ConfigurationValidationException {
        configurator.init(BaseSelectors.find(qualifiedName), object);
    }

    /** {@inheritDoc} */
    @Override public void changeWithoutValidation(T object) {
        this.val = object;

        for (PropertyListener<T, T, T> listener : updateListeners) {
            listener.update(object, this);
        }
    }

    /** {@inheritDoc} */
    @Override public void initWithoutValidation(T object) {
        this.val = object;
    }

    /** {@inheritDoc} */
    @Override public void validate(DynamicConfiguration<?, ?, ?> oldRoot) throws ConfigurationValidationException {
        final List<? extends FieldValidator<? extends Serializable, ? extends DynamicConfiguration<?, ?, ?>>> validators = configurator.validators(memberKey);

        for (FieldValidator<? extends Serializable, ? extends DynamicConfiguration<?, ?, ?>> validator : validators)
            ((FieldValidator<T, DynamicConfiguration<?, ?, ?>>) validator).validate(val, root, oldRoot);
    }

    /** {@inheritDoc} */
    @Override public String key() {
        return name;
    }

    /**
     * Get fully qualified name of this property.
     * @return Fully qualified name.
     */
    public String qualifiedName() {
        return qualifiedName;
    }

    public void setSilently(T serializable) {
        val = serializable;

        for (PropertyListener<T, T, T> listener : updateListeners) {
            listener.update(val, this);
        }
    }

    /**
     * Create a deep copy of this DynamicProperty, but attaching it to another configuration root.
     * @param newRoot New configuration root.
     * @return Copy of this property.
     */
    public DynamicProperty<T> copy(DynamicConfiguration<?, ?, ?> newRoot) {
        return new DynamicProperty<>(this, newRoot);
    }

}
