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

package org.apache.ignite.internal.configuration;

import static java.util.Collections.emptyList;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CompoundModuleTest extends BaseIgniteAbstractTest {
    @Mock
    private RootKey<?, ?, ?> rootKeyA;
    @Mock
    private RootKey<?, ?, ?> rootKeyB;
    @Mock
    private RootKey<?, ?, ?> rootKeyC;
    @Mock
    private RootKey<?, ?, ?> rootKeyD;

    @Mock
    private Validator<AnnotationA, ?> validatorA;
    @Mock
    private Validator<AnnotationB, ?> validatorB;

    @Mock
    private ConfigurationModule moduleA;
    @Mock
    private ConfigurationModule moduleB;
    @Mock
    private ConfigurationModule moduleC;
    @Mock
    private ConfigurationModule moduleD;

    private ConfigurationModule compound;

    @BeforeEach
    void createCompoundModule() {
        when(moduleA.type()).thenReturn(LOCAL);
        when(moduleB.type()).thenReturn(LOCAL);

        compound = CompoundModule.ofType(LOCAL, List.of(moduleA, moduleB));
    }

    @Test
    void returnsTypePassedViaFactory() {
        ConfigurationModule compound = CompoundModule.ofType(LOCAL, emptyList());

        assertThat(compound.type(), is(LOCAL));
    }

    @Test
    void returnsUnionOfRootKeysOfItsModules() {
        when(moduleA.rootKeys()).thenReturn(Set.of(rootKeyA));
        when(moduleB.rootKeys()).thenReturn(Set.of(rootKeyB));

        assertThat(compound.rootKeys(), containsInAnyOrder(rootKeyA, rootKeyB));
    }

    @Test
    void providesUnionOfValidatorsMapsOfItsModulesPerKey() {
        when(moduleA.validators()).thenReturn(Set.of(validatorA));
        when(moduleB.validators()).thenReturn(Set.of(validatorB));

        Set<Validator<?, ?>> validators = compound.validators();

        assertThat(validators, hasItem(validatorA));
        assertThat(validators, hasItem(validatorB));
    }

    @Test
    void mergesValidatorsUnderTheSameKey() {
        when(moduleA.validators()).thenReturn(Set.of(validatorA));
        when(moduleB.validators()).thenReturn(Set.of(validatorB));

        Set<Validator<?, ?>> validators = compound.validators();

        assertThat(validators, hasItems(validatorA, validatorB));
    }

    @Test
    void returnsUnionOfInternalSchemaExtensionsOfItsModules() {
        when(moduleA.schemaExtensions()).thenReturn(Set.of(ExtensionA.class));
        when(moduleB.schemaExtensions()).thenReturn(Set.of(ExtensionB.class));

        assertThat(compound.schemaExtensions(), containsInAnyOrder(ExtensionA.class, ExtensionB.class));
    }

    @Test
    void returnsUnionOfPolymorphicSchemaExtensionsOfItsModules() {
        when(moduleA.polymorphicSchemaExtensions()).thenReturn(Set.of(ExtensionA.class));
        when(moduleB.polymorphicSchemaExtensions()).thenReturn(Set.of(ExtensionB.class));

        assertThat(compound.polymorphicSchemaExtensions(), containsInAnyOrder(ExtensionA.class, ExtensionB.class));
    }

    @Test
    void localFiltersOnlyLocalModules() {
        when(moduleA.type()).thenReturn(LOCAL);
        when(moduleB.type()).thenReturn(LOCAL);
        when(moduleC.type()).thenReturn(DISTRIBUTED);
        when(moduleD.type()).thenReturn(DISTRIBUTED);
        when(moduleA.rootKeys()).thenReturn(Set.of(rootKeyA));
        when(moduleB.rootKeys()).thenReturn(Set.of(rootKeyB));

        ConfigurationModule local = CompoundModule.local(List.of(moduleA, moduleB, moduleC, moduleD));

        assertThat(local.rootKeys(), containsInAnyOrder(rootKeyA, rootKeyB));
    }

    @Test
    void distributedFiltersOnlyDistributedModules() {
        when(moduleA.type()).thenReturn(LOCAL);
        when(moduleB.type()).thenReturn(LOCAL);
        when(moduleC.type()).thenReturn(DISTRIBUTED);
        when(moduleD.type()).thenReturn(DISTRIBUTED);
        when(moduleC.rootKeys()).thenReturn(Set.of(rootKeyC));
        when(moduleD.rootKeys()).thenReturn(Set.of(rootKeyD));

        ConfigurationModule distributed = CompoundModule.distributed(List.of(moduleA, moduleB, moduleC, moduleD));

        assertThat(distributed.rootKeys(), containsInAnyOrder(rootKeyC, rootKeyD));
    }

    @Test
    void localAndDistributedFilterByType() {
        when(moduleA.type()).thenReturn(LOCAL);
        when(moduleB.type()).thenReturn(DISTRIBUTED);
        when(moduleA.schemaExtensions()).thenReturn(Set.of(ExtensionA.class));
        when(moduleB.schemaExtensions()).thenReturn(Set.of(ExtensionB.class));

        List<ConfigurationModule> allModules = List.of(moduleA, moduleB);

        assertThat(CompoundModule.local(allModules).schemaExtensions(), containsInAnyOrder(ExtensionA.class));
        assertThat(CompoundModule.distributed(allModules).schemaExtensions(), containsInAnyOrder(ExtensionB.class));
    }

    private @interface AnnotationA {
    }

    private @interface AnnotationB {
    }

    private static class ExtensionA {
    }

    private static class ExtensionB {
    }
}
