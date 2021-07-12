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

package org.apache.ignite.internal.configuration.tree;

import java.util.Map;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.TestConfigurationChanger;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NamedListOrderTest {
    /** */
    @ConfigurationRoot(rootName = "a")
    public static class AConfigurationSchema {
        /** */
        @NamedConfigValue
        public BConfigurationSchema b;
    }

    /** */
    @Config
    public static class BConfigurationSchema {
        /** */
        @Value(hasDefault = true)
        public String c = "foo";

        @NamedConfigValue
        public BConfigurationSchema b;
    }

    private static ConfigurationAsmGenerator cgen;

    private TestConfigurationStorage storage;

    private ConfigurationChanger changer;

    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();
    }

    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    @BeforeEach
    public void before() {
        storage = new TestConfigurationStorage();

        changer = new TestConfigurationChanger(cgen);
        changer.addRootKey(AConfiguration.KEY);
        changer.register(storage);
    }

    @AfterEach
    public void after() {
        changer.stop();
    }

    @Test
    public void test() throws Exception {
        AConfiguration a = (AConfiguration)cgen.instantiateCfg(AConfiguration.KEY, changer);

        a.b().change(b -> b.create("X", x -> x.changeB(xb -> xb.create("Z0", z0 -> {})))).get();

        assertEquals(
            Map.of(
                "a.b.X.c", "foo",
                "a.b.X.<idx>", 0,
                "a.b.X.b.Z0.c", "foo",
                "a.b.X.b.Z0.<idx>", 0
            ),
            storage.readAll().values()
        );

        BConfiguration x = a.b().get("X");

        x.b().change(xb -> xb.create("Z5", z5 -> {})).get();

        assertEquals(
            Map.of(
                "a.b.X.c", "foo",
                "a.b.X.<idx>", 0,
                "a.b.X.b.Z0.c", "foo",
                "a.b.X.b.Z0.<idx>", 0,
                "a.b.X.b.Z5.c", "foo",
                "a.b.X.b.Z5.<idx>", 1
            ),
            storage.readAll().values()
        );

        x.b().change(xb -> xb.create(1, "Z2", z2 -> {})).get();

        assertEquals(
            Map.of(
                "a.b.X.c", "foo",
                "a.b.X.<idx>", 0,
                "a.b.X.b.Z0.c", "foo",
                "a.b.X.b.Z0.<idx>", 0,
                "a.b.X.b.Z2.c", "foo",
                "a.b.X.b.Z2.<idx>", 1,
                "a.b.X.b.Z5.c", "foo",
                "a.b.X.b.Z5.<idx>", 2
            ),
            storage.readAll().values()
        );

        x.b().change(xb -> xb.createAfter("Z2", "Z3", z3 -> {})).get();

        assertEquals(
            Map.of(
                "a.b.X.c", "foo",
                "a.b.X.<idx>", 0,
                "a.b.X.b.Z0.c", "foo",
                "a.b.X.b.Z0.<idx>", 0,
                "a.b.X.b.Z2.c", "foo",
                "a.b.X.b.Z2.<idx>", 1,
                "a.b.X.b.Z3.c", "foo",
                "a.b.X.b.Z3.<idx>", 2,
                "a.b.X.b.Z5.c", "foo",
                "a.b.X.b.Z5.<idx>", 3
            ),
            storage.readAll().values()
        );

        x.b().change(xb -> xb.delete("Z2")).get();

        assertEquals(
            Map.of(
                "a.b.X.c", "foo",
                "a.b.X.<idx>", 0,
                "a.b.X.b.Z0.c", "foo",
                "a.b.X.b.Z0.<idx>", 0,
                "a.b.X.b.Z3.c", "foo",
                "a.b.X.b.Z3.<idx>", 1,
                "a.b.X.b.Z5.c", "foo",
                "a.b.X.b.Z5.<idx>", 2
            ),
            storage.readAll().values()
        );

        a.b().change(b -> b.delete("X")).get();

        assertEquals(
            Map.of(),
            storage.readAll().values()
        );
    }
}
