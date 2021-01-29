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
package org.apache.ignite.configuration.sample.configuration;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.configuration.ConfigurationChanger;
import org.apache.ignite.configuration.Configurator;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.sample.configuration.impl.ANode;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfigurationStorageTest {

    private static final RootKey<?> KEY = () -> "key";
    /** */
    @Config
    public static class AConfigurationSchema {
        /** */
        @ConfigValue
        private BConfigurationSchema child;

        /** */
        @NamedConfigValue
        private CConfigurationSchema elements;
    }

    /** */
    @Config
    public static class BConfigurationSchema {
        /** */
        @Value(immutable = true)
        private int intCfg;

        /** */
        @Value
        private String strCfg;
    }

    /** */
    @Config
    public static class CConfigurationSchema {
        /** */
        @Value
        private String strCfg;
    }

    @Test
    public void testPutGet() {
        final TestConfigurationStorage storage = new TestConfigurationStorage();

        final DynamicConfigurationController con = new DynamicConfigurationController();
        final Configurator<?> configuration = con.getConfiguration();

        ANode data = new ANode()
            .initChild(init -> init.initIntCfg(1).initStrCfg("1"))
            .initElements(change -> change.put("a", init -> init.initStrCfg("1")));

        final ConfigurationChanger changer = new ConfigurationChanger(storage);
        changer.init();

        changer.registerConfiguration(KEY, configuration);

        changer.change(Collections.singletonMap(KEY, data));

        final Data dataFromStorage = storage.readAll();
        final Map<String, Serializable> dataMap = dataFromStorage.values();

        assertEquals(3, dataMap.size());
        assertThat(dataMap, hasEntry("key.child.intCfg", 1));
        assertThat(dataMap, hasEntry("key.child.strCfg", "1"));
        assertThat(dataMap, hasEntry("key.elements.a.strCfg", "1"));
    }

    @Test
    public void testModifiedFromAnotherStorage() {
        final TestConfigurationStorage.Storage singleSource = new TestConfigurationStorage.Storage();

        final TestConfigurationStorage storage1 = new TestConfigurationStorage(singleSource);
        final TestConfigurationStorage storage2 = new TestConfigurationStorage(singleSource);

        final DynamicConfigurationController con = new DynamicConfigurationController();
        final Configurator<?> configuration = con.getConfiguration();

        ANode data1 = new ANode()
            .initChild(init -> init.initIntCfg(1).initStrCfg("1"))
            .initElements(change -> change.put("a", init -> init.initStrCfg("1")));

        ANode data2 = new ANode()
            .initChild(init -> init.initIntCfg(2).initStrCfg("2"))
            .initElements(change -> change
                .put("a", init -> init.initStrCfg("2"))
                .put("b", init -> init.initStrCfg("2"))
            );

        final ConfigurationChanger changer1 = new ConfigurationChanger(storage1);
        changer1.init();

        final ConfigurationChanger changer2 = new ConfigurationChanger(storage2);
        changer2.init();

        changer1.registerConfiguration(KEY, configuration);
        changer2.registerConfiguration(KEY, configuration);

        changer1.change(Collections.singletonMap(KEY, data1));
        changer2.change(Collections.singletonMap(KEY, data2));

        final Data dataFromStorage = storage1.readAll();
        final Map<String, Serializable> dataMap = dataFromStorage.values();

        assertEquals(4, dataMap.size());
        assertThat(dataMap, hasEntry("key.child.intCfg", 2));
        assertThat(dataMap, hasEntry("key.child.strCfg", "2"));
        assertThat(dataMap, hasEntry("key.elements.a.strCfg", "2"));
        assertThat(dataMap, hasEntry("key.elements.b.strCfg", "2"));
    }

    @Test
    public void testModifiedFromAnotherStorageWithIncompatibleChanges() {
        final TestConfigurationStorage.Storage singleSource = new TestConfigurationStorage.Storage();

        final TestConfigurationStorage storage1 = new TestConfigurationStorage(singleSource);
        final TestConfigurationStorage storage2 = new TestConfigurationStorage(singleSource);

        final DynamicConfigurationController con = new DynamicConfigurationController();

        final Configurator<?> configuration = con.getConfiguration();

        ANode data1 = new ANode()
            .initChild(init -> init.initIntCfg(1).initStrCfg("1"))
            .initElements(change -> change.put("a", init -> init.initStrCfg("1")));

        ANode data2 = new ANode()
            .initChild(init -> init.initIntCfg(2).initStrCfg("2"))
            .initElements(change -> change
                .put("a", init -> init.initStrCfg("2"))
                .put("b", init -> init.initStrCfg("2"))
            );

        final ConfigurationChanger changer1 = new ConfigurationChanger(storage1);
        changer1.init();

        final ConfigurationChanger changer2 = new ConfigurationChanger(storage2);
        changer2.init();

        changer1.registerConfiguration(KEY, configuration);
        changer2.registerConfiguration(KEY, configuration);

        changer1.change(Collections.singletonMap(KEY, data1));

        con.setHasIssues(true);

        assertThrows(ConfigurationValidationException.class, () -> changer2.change(Collections.singletonMap(KEY, data2)));

        final Data dataFromStorage = storage1.readAll();
        final Map<String, Serializable> dataMap = dataFromStorage.values();

        assertEquals(3, dataMap.size());
        assertThat(dataMap, hasEntry("key.child.intCfg", 1));
        assertThat(dataMap, hasEntry("key.child.strCfg", "1"));
        assertThat(dataMap, hasEntry("key.elements.a.strCfg", "1"));
    }

    private static class DynamicConfigurationController {

        final Configurator<?> configuration;

        private boolean hasIssues;

        public DynamicConfigurationController() {
            this(false);
        }

        public DynamicConfigurationController(boolean hasIssues) {
            this.hasIssues = hasIssues;

            configuration = Mockito.mock(Configurator.class);

            Mockito.when(configuration.validateChanges(Mockito.any())).then(mock -> {
                if (this.hasIssues)
                    return Collections.singletonList(new ValidationIssue());

                return Collections.emptyList();
            });
        }

        public void setHasIssues(boolean hasIssues) {
            this.hasIssues = hasIssues;
        }

        public Configurator<?> getConfiguration() {
            return configuration;
        }
    }

}
