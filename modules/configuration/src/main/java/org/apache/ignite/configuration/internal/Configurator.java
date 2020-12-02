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
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.configuration.internal.property.DynamicProperty;
import org.apache.ignite.configuration.internal.property.Modifier;
import org.apache.ignite.configuration.internal.property.PropertyListener;
import org.apache.ignite.configuration.internal.selector.Selector;
import org.apache.ignite.configuration.internal.validation.FieldValidator;
import org.apache.ignite.configuration.internal.validation.MemberKey;

public class Configurator<T extends DynamicConfiguration<?, ?, ?>> {

    private final ConfigurationStorage storage;

    private final T root;

    private final Map<MemberKey, List<FieldValidator<? extends Serializable, ? extends DynamicConfiguration<?, ?, ?>>>> fieldValidators = new HashMap<>();

    public Configurator(ConfigurationStorage storage, Function<Configurator<T>, T> rootBuilder) {
        this.storage = storage;
        this.root = rootBuilder.apply(this);
        this.init();
    }

    private void init() {
        List<DynamicProperty> props = new ArrayList<>();

        props.forEach(property -> {
            final String key = property.key();
            property.addListener(new PropertyListener() {
                @Override public void update(Serializable newValue, Modifier modifier) {
                    storage.save(key, newValue);
                }
            });
            storage.listen(key, serializable -> {
                property.setSilently(serializable);
            });
        });
    }

    public <TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> VIEW getPublic(Selector<T, TARGET, VIEW, INIT, CHANGE> selector) {
        return selector.select(root).toView();
    }

    public <TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> void set(Selector<T, TARGET, VIEW, INIT, CHANGE> selector, CHANGE newValue) {
        // TODO: atomic change start
        final T copy = (T) root.copy();

        final TARGET select = selector.select(copy);
        select.changeWithoutValidation(newValue);
        copy.validate(root);
        selector.select(root).changeWithoutValidation(newValue);
        // TODO: atomic change end
    }

    public <TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> void init(Selector<T, TARGET, VIEW, INIT, CHANGE> selector, INIT initValue) {
        final TARGET select = selector.select(root);
        select.initWithoutValidation(initValue);
        root.validate(root);
    }

    public <TARGET extends Modifier<VIEW, INIT, CHANGE>, VIEW, INIT, CHANGE> TARGET getInternal(Selector<T, TARGET, VIEW, INIT, CHANGE> selector) {
        return selector.select(root);
    }

    public <PROP extends Serializable> void addValidations(
        Class<? extends DynamicConfiguration<?, ?, ?>> aClass,
        String key,
        List<FieldValidator<? super PROP, ? extends DynamicConfiguration<?, ?, ?>>> validators
    ) {
        fieldValidators.put(new MemberKey(aClass, key), (List) validators);
    }

    public List<FieldValidator<? extends Serializable, ? extends DynamicConfiguration<?, ?, ?>>> validators(MemberKey key) {
        return fieldValidators.getOrDefault(key, Collections.emptyList());
    }

    public T getRoot() {
        return root;
    }

}
