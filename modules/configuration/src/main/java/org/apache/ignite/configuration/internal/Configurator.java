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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import org.apache.ignite.configuration.internal.property.DynamicProperty;
import org.apache.ignite.configuration.internal.property.Modifier;
import org.apache.ignite.configuration.internal.property.PropertyListener;
import org.apache.ignite.configuration.internal.selector.Selector;
import org.apache.ignite.configuration.internal.storage.ConfigurationStorage;
import org.apache.ignite.configuration.internal.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.internal.validation.FieldValidator;
import org.apache.ignite.configuration.internal.validation.MemberKey;

/**
 * Convenient wrapper for configuration root. Provides access to configuration tree, stores validators, performs actions
 * on configuration such as initialized, change and view.
 * @param <T> Type of configuration root.
 */
public class Configurator<T extends DynamicConfiguration<?, ?, ?>> {
    /** Storage for the configuration tree. */
    private final ConfigurationStorage storage;

    /** Root of the configuration tree. */
    private final T root;

    /** Configuration property validators. */
    private final Map<MemberKey, List<FieldValidator<? extends Serializable, T>>> fieldValidators = new HashMap<>();

    /**
     * Constructor.
     * @param storage Configuration storage.
     * @param rootBuilder Function, that creates configuration root.
     */
    public Configurator(ConfigurationStorage storage, Function<Configurator<T>, T> rootBuilder) {
        this.storage = storage;
        this.root = rootBuilder.apply(this);
        this.init();
    }

    /**
     * Initialize.
     */
    private void init() {
        List<DynamicProperty<?>> props = new ArrayList<>();

        Queue<DynamicConfiguration<?, ?, ?>> confs = new LinkedList<>();
        confs.add(root);
        while (!confs.isEmpty()) {
            final DynamicConfiguration<?, ?, ?> conf = confs.poll();
            for (Object modifier : conf.members.values()) {
                if (modifier instanceof DynamicConfiguration) {
                    confs.add((DynamicConfiguration<?, ?, ?>) modifier);
                } else {
                    props.add((DynamicProperty<?>) modifier);
                }
            }
        }

        for (DynamicProperty property : props) {
            final String key = property.key();
            property.addListener(new PropertyListener() {
                /** {@inheritDoc} */
                @Override public void update(Serializable newValue, Modifier modifier) {
                    storage.save(key, newValue);
                }
            });
            storage.listen(key, serializable -> {
                property.setSilently(serializable);
            });
        }
    }

    /**
     *
     * @param selector
     * @param <TARGET>
     * @param <VIEW>
     * @param <INIT>
     * @param <CHANGE>
     * @return
     */
    public <TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> VIEW getPublic(Selector<T, TARGET, VIEW, INIT, CHANGE> selector) {
        return selector.select(root).toView();
    }

    /**
     *
     * @param selector
     * @param newValue
     * @param <TARGET>
     * @param <VIEW>
     * @param <INIT>
     * @param <CHANGE>
     * @throws ConfigurationValidationException
     */
    public <TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> void set(Selector<T, TARGET, VIEW, INIT, CHANGE> selector, CHANGE newValue) throws ConfigurationValidationException {
        // TODO: atomic change start
        final T copy = (T) root.copy();

        final TARGET select = selector.select(copy);
        select.changeWithoutValidation(newValue);
        copy.validate(root);
        selector.select(root).changeWithoutValidation(newValue);
        // TODO: atomic change end
    }

    /**
     *
     * @param selector
     * @param initValue
     * @param <TARGET>
     * @param <VIEW>
     * @param <INIT>
     * @param <CHANGE>
     * @throws ConfigurationValidationException
     */
    public <TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> void init(Selector<T, TARGET, VIEW, INIT, CHANGE> selector, INIT initValue) throws ConfigurationValidationException {
        final TARGET select = selector.select(root);
        select.initWithoutValidation(initValue);
        root.validate(root);
    }

    /**
     *
     * @param selector
     * @param <TARGET>
     * @param <VIEW>
     * @param <INIT>
     * @param <CHANGE>
     * @return
     */
    public <TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> TARGET getInternal(Selector<T, TARGET, VIEW, INIT, CHANGE> selector) {
        return selector.select(root);
    }

    /**
     *
     * @param aClass
     * @param key
     * @param validators
     * @param <PROP>
     */
    public <PROP extends Serializable> void addValidations(
        Class<? extends DynamicConfiguration<?, ?, ?>> aClass,
        String key,
        List<FieldValidator<? super PROP, ? extends DynamicConfiguration<?, ?, ?>>> validators
    ) {
        fieldValidators.put(new MemberKey(aClass, key), (List) validators);
    }

    /**
     * Get all validators for given member key (class + field).
     * @param key Member key.
     * @return Validators.
     */
    public List<FieldValidator<? extends Serializable, T>> validators(MemberKey key) {
        return fieldValidators.getOrDefault(key, Collections.emptyList());
    }

    /**
     * Get configuration root.
     * @return Configuration root.
     */
    public T getRoot() {
        return root;
    }

}
