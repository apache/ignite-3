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

package org.apache.ignite.internal.configuration;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.ConfigurationModule;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfigurationSchema;

/**
 *
 */
public class TableDistributedConfigurationModule implements ConfigurationModule {
    @Override
    public ConfigurationType type() {
        return ConfigurationType.DISTRIBUTED;
    }

    @Override
    public Collection<RootKey<?, ?>> rootKeys() {
        return emptyList();
    }

    @Override
    public Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> validators() {
        return emptyMap();
    }

    @Override
    public Collection<Class<?>> internalSchemaExtensions() {
        return List.of(ExtendedTableConfigurationSchema.class);
    }

    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return emptyList();
    }
}
