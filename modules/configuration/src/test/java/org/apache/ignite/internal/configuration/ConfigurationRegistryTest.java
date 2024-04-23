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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.junit.jupiter.api.Test;

/**
 * Class for testing the {@link ConfigurationRegistry}.
 */
public class ConfigurationRegistryTest {
    @Test
    void testValidationInternalConfigurationExtensions() throws Exception {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ConfigurationRegistry(
                        List.of(SecondRootConfiguration.KEY),
                        new TestConfigurationStorage(LOCAL),
                        new ConfigurationTreeGenerator(
                                List.of(SecondRootConfiguration.KEY),
                                List.of(ExtendedFirstRootConfigurationSchema.class),
                                List.of()
                        ),
                        new TestConfigurationValidator()
                )
        );

        // Check that everything is fine.
        ConfigurationRegistry configRegistry = new ConfigurationRegistry(
                List.of(FirstRootConfiguration.KEY, SecondRootConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                new ConfigurationTreeGenerator(
                        List.of(FirstRootConfiguration.KEY, SecondRootConfiguration.KEY),
                        List.of(ExtendedFirstRootConfigurationSchema.class),
                        List.of()
                ),
                new TestConfigurationValidator()
        );

        assertThat(configRegistry.stopAsync(), willCompleteSuccessfully());
    }

    @Test
    void testValidationPolymorphicConfigurationExtensions() throws Exception {
        // There is a polymorphic extension that is missing from the schema.
        assertThrows(
                IllegalArgumentException.class,
                () -> new ConfigurationRegistry(
                        List.of(ThirdRootConfiguration.KEY),
                        new TestConfigurationStorage(LOCAL),
                        new ConfigurationTreeGenerator(
                                List.of(ThirdRootConfiguration.KEY),
                                List.of(),
                                List.of(Second0PolymorphicConfigurationSchema.class)
                        ),
                        new TestConfigurationValidator()
                )
        );

        // There are two polymorphic extensions with the same id.
        assertThrows(
                IllegalArgumentException.class,
                () -> new ConfigurationRegistry(
                        List.of(ThirdRootConfiguration.KEY),
                        new TestConfigurationStorage(LOCAL),
                        new ConfigurationTreeGenerator(
                                List.of(ThirdRootConfiguration.KEY),
                                List.of(),
                                List.of(First0PolymorphicConfigurationSchema.class, ErrorFirst0PolymorphicConfigurationSchema.class)
                        ),
                        new TestConfigurationValidator()
                )
        );

        // Check that everything is fine.
        ConfigurationRegistry configRegistry = new ConfigurationRegistry(
                List.of(ThirdRootConfiguration.KEY, FourthRootConfiguration.KEY, FifthRootConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                new ConfigurationTreeGenerator(
                        List.of(ThirdRootConfiguration.KEY, FourthRootConfiguration.KEY, FifthRootConfiguration.KEY),
                        List.of(),
                        List.of(
                                First0PolymorphicConfigurationSchema.class,
                                First1PolymorphicConfigurationSchema.class,
                                Second0PolymorphicConfigurationSchema.class,
                                Third0PolymorphicConfigurationSchema.class,
                                Third1PolymorphicConfigurationSchema.class
                        )
                ),
                new TestConfigurationValidator()
        );

        assertThat(configRegistry.stopAsync(), willCompleteSuccessfully());
    }

    @Test
    void missingPolymorphicExtension() {
        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> new ConfigurationRegistry(
                        List.of(ThirdRootConfiguration.KEY),
                        new TestConfigurationStorage(LOCAL),
                        new ConfigurationTreeGenerator(ThirdRootConfiguration.KEY),
                        new TestConfigurationValidator()
                )
        );

        assertThat(ex.getMessage(), is("Polymorphic configuration schemas for which no extensions were found: "
                + "[class org.apache.ignite.internal.configuration.ConfigurationRegistryTest$"
                + "FirstPolymorphicConfigurationSchema]"));
    }

    @Test
    void testComplicatedPolymorphicConfig() throws Exception {

        Map<String, Serializable> bootstrapConfig = Map.of(
                "sixth.entity.poly.strVal", "val",
                "sixth.poly.strVal", "val",
                "sixth.entity.poly.intVal", 1,
                "sixth.poly.intVal", 1
        );

        ConfigurationRegistry registry = new ConfigurationRegistry(
                List.of(SixthRootConfiguration.KEY),
                new TestConfigurationStorage(LOCAL, bootstrapConfig),
                new ConfigurationTreeGenerator(
                        List.of(SixthRootConfiguration.KEY),
                        List.of(),
                        List.of(Fourth0PolymorphicConfigurationSchema.class)
                ),
                new TestConfigurationValidator()
        );

        assertThat(registry.startAsync(), willCompleteSuccessfully());

        try {
            registry.getConfiguration(SixthRootConfiguration.KEY).change(c -> c
                    .changePoly(toFirst0Polymorphic(0))
                    .changePolyNamed(c0 -> c0.create("1", toFirst0Polymorphic(1)))
                    .changeEntity(c0 -> c0.changePoly(toFirst0Polymorphic(2))
                            .changePolyNamed(c1 -> c1.create("3", toFirst0Polymorphic(3))))
                    .changeEntityNamed(c0 -> c0.create("4",
                            c1 -> c1.changePoly(toFirst0Polymorphic(4))
                                    .changePolyNamed(c2 -> c2.create("5", toFirst0Polymorphic(5)))))
            ).get(1, SECONDS);
        } finally {
            assertThat(registry.stopAsync(), willCompleteSuccessfully());
        }
    }

    @Test
    void testPolymorphicGet() throws Exception {
        ConfigurationRegistry registry = new ConfigurationRegistry(
                List.of(SixthRootConfiguration.KEY),
                new TestConfigurationStorage(LOCAL),
                new ConfigurationTreeGenerator(
                        List.of(SixthRootConfiguration.KEY),
                        List.of(),
                        List.of(
                                Fourth0PolymorphicConfigurationSchema.class
                        )
                ),
                new TestConfigurationValidator()
        );
        assertThat(registry.startAsync(), willCompleteSuccessfully());

        var configuration = registry.getConfiguration(SixthRootConfiguration.KEY).polyNamed();
        CompletableFuture<Void> future = configuration.change(c -> {
            c.create(
                    "fourth0",
                    fourthPolymorphicChange -> fourthPolymorphicChange.convert(Fourth0PolymorphicChange.class)
            );
        });

        assertThat(future, willCompleteSuccessfully());

        UUID internalId = configuration.internalIds().get(0);
        assertThat(configuration.get(internalId), instanceOf(Fourth0PolymorphicConfiguration.class));

        assertThat(registry.stopAsync(), willCompleteSuccessfully());
    }

    @Test
    void testChangeSuperRoot() throws Exception {
        TestConfigurationStorage storage = new TestConfigurationStorage(LOCAL);

        var registry = new ConfigurationRegistry(
                List.of(FirstRootConfiguration.KEY, SecondRootConfiguration.KEY),
                storage,
                new ConfigurationTreeGenerator(FirstRootConfiguration.KEY, SecondRootConfiguration.KEY),
                new TestConfigurationValidator()
        );

        assertThat(registry.startAsync(), willCompleteSuccessfully());

        try {
            FirstRootConfiguration firstConfiguration = registry.getConfiguration(FirstRootConfiguration.KEY);
            SecondRootConfiguration secondConfiguration = registry.getConfiguration(SecondRootConfiguration.KEY);

            CompletableFuture<Void> changeFuture = registry.change(superRootChange -> {
                assertNotNull(superRootChange);

                // Check that originally we have the same value for the root.
                assertSame(firstConfiguration.value(), superRootChange.viewRoot(FirstRootConfiguration.KEY));

                FirstRootChange firstRootChange = superRootChange.changeRoot(FirstRootConfiguration.KEY);

                // Check that the value of the root has changed.
                assertNotSame(firstConfiguration.value(), firstRootChange);
                assertSame(firstRootChange, superRootChange.viewRoot(FirstRootConfiguration.KEY));

                firstRootChange.changeStr("foo");
                superRootChange.changeRoot(SecondRootConfiguration.KEY).changeStr("bar");
            });

            assertThat(changeFuture, willCompleteSuccessfully());

            assertEquals("foo", firstConfiguration.str().value());
            assertEquals("bar", secondConfiguration.str().value());
        } finally {
            assertThat(registry.stopAsync(), willCompleteSuccessfully());
        }
    }

    private Consumer<FourthPolymorphicChange> toFirst0Polymorphic(int v) {
        return c -> c.convert(Fourth0PolymorphicChange.class).changeIntVal(v).changeStrVal(Integer.toString(v));
    }

    /**
     * First root configuration.
     */
    @ConfigurationRoot(rootName = "first")
    public static class FirstRootConfigurationSchema {
        @Value(hasDefault = true)
        public String str = "str";
    }

    /**
     * Second root configuration.
     */
    @ConfigurationRoot(rootName = "second")
    public static class SecondRootConfigurationSchema {
        @Value(hasDefault = true)
        public String str = "str";
    }

    /**
     * First extended root configuration.
     */
    @ConfigurationExtension(internal = true)
    public static class ExtendedFirstRootConfigurationSchema extends FirstRootConfigurationSchema {
        @Value(hasDefault = true)
        public String strEx = "str";
    }

    /**
     * Third root configuration.
     */
    @ConfigurationRoot(rootName = "third")
    public static class ThirdRootConfigurationSchema {
        @ConfigValue
        public FirstPolymorphicConfigurationSchema polymorphicConfig;
    }

    /**
     * Fourth root configuration.
     */
    @ConfigurationRoot(rootName = "fourth")
    public static class FourthRootConfigurationSchema {
        @ConfigValue
        public SecondPolymorphicConfigurationSchema polymorphicConfig;
    }

    /**
     * Fifth root configuration.
     */
    @ConfigurationRoot(rootName = "fifth")
    public static class FifthRootConfigurationSchema {
        @ConfigValue
        public ThirdPolymorphicConfigurationSchema polymorphicConfig;
    }

    /**
     * Sixth root configuration.
     */
    @ConfigurationRoot(rootName = "sixth")
    public static class SixthRootConfigurationSchema {
        @ConfigValue
        public FourthPolymorphicConfigurationSchema poly;

        @NamedConfigValue
        public FourthPolymorphicConfigurationSchema polyNamed;

        @ConfigValue
        public EntityConfigurationSchema entity;

        @NamedConfigValue
        public EntityConfigurationSchema entityNamed;
    }

    /**
     * Inner configuration schema.
     */
    @Config
    public static class EntityConfigurationSchema {
        @ConfigValue
        public FourthPolymorphicConfigurationSchema poly;

        @NamedConfigValue
        public FourthPolymorphicConfigurationSchema polyNamed;
    }

    /**
     * Simple first polymorphic configuration scheme.
     */
    @PolymorphicConfig
    public static class FirstPolymorphicConfigurationSchema {
        @PolymorphicId
        public String typeId;
    }

    /**
     * First {@link FirstPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("first0")
    public static class First0PolymorphicConfigurationSchema extends FirstPolymorphicConfigurationSchema {
    }

    /**
     * Second {@link FirstPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("first1")
    public static class First1PolymorphicConfigurationSchema extends FirstPolymorphicConfigurationSchema {
    }

    /**
     * First error {@link FirstPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("first0")
    public static class ErrorFirst0PolymorphicConfigurationSchema extends FirstPolymorphicConfigurationSchema {
    }

    /**
     * Second polymorphic configuration scheme.
     */
    @PolymorphicConfig
    public static class SecondPolymorphicConfigurationSchema {
        @PolymorphicId
        public String typeId;
    }

    /**
     * First {@link SecondPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("second0")
    public static class Second0PolymorphicConfigurationSchema extends SecondPolymorphicConfigurationSchema {
    }

    /**
     * Third polymorphic configuration scheme.
     */
    @PolymorphicConfig
    public static class ThirdPolymorphicConfigurationSchema {
        @PolymorphicId(hasDefault = true)
        public String typeId = "third0";
    }

    /**
     * First {@link ThirdPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("third0")
    public static class Third0PolymorphicConfigurationSchema extends ThirdPolymorphicConfigurationSchema {
    }

    /**
     * Second {@link ThirdPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("third1")
    public static class Third1PolymorphicConfigurationSchema extends ThirdPolymorphicConfigurationSchema {
    }

    /**
     * Fourth polymorphic configuration scheme.
     */
    @PolymorphicConfig
    public static class FourthPolymorphicConfigurationSchema {
        @PolymorphicId(hasDefault = true)
        public String typeId = "fourth0";

        @Value(hasDefault = true)
        public String strVal = "";
    }

    /**
     * First {@link FourthPolymorphicConfigurationSchema} extension.
     */
    @PolymorphicConfigInstance("fourth0")
    public static class Fourth0PolymorphicConfigurationSchema extends FourthPolymorphicConfigurationSchema {
        @Value(hasDefault = true)
        public int intVal = 0;
    }
}
