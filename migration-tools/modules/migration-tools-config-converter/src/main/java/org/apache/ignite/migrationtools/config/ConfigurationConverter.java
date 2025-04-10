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

package org.apache.ignite.migrationtools.config;

import static org.mockito.internal.util.MockUtil.typeMockabilityOf;

import java.beans.PropertyDescriptor;
import java.io.File;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.migrationtools.config.converters.ClientConnectorConverter;
import org.apache.ignite.migrationtools.config.converters.DataRegionConfigurationConverter;
import org.apache.ignite.migrationtools.config.converters.DiscoverySpiConverter;
import org.apache.ignite.migrationtools.config.converters.SslContextFactoryConverter;
import org.apache.ignite.migrationtools.config.loader.CustomBeanFactory;
import org.jetbrains.annotations.Nullable;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.core.io.FileSystemResource;

/** ConfigurationConverter. */
public class ConfigurationConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationConverter.class);

    // TODO: Make this injected.
    private static List<org.apache.ignite.migrationtools.config.converters.ConfigurationConverter> converters = List.of(
            // new CommunicationSpiConverter(), Disabled the TcpCommSpi. Will use the port from the Discovery Module
            new DiscoverySpiConverter(),
            new SslContextFactoryConverter(),
            new ClientConnectorConverter(),
            new DataRegionConfigurationConverter()
    );

    /**
     * Converts an Apache Ignite 2 configuration file into its Apache Ignite 3 node and cluster configurations counterparts.
     *
     * @param inputFile Apache Ignite 2 configuration file.
     * @param nodeCfgFile Target Ignite 3 node configuration file.
     * @param clusterCfgFile Target Ignite 3 cluster configuration file.
     * @param includeDefaults Whether defaults will be included in the target files.
     * @param clientClassloader Custom classloader to load any additional 3rd party libraries (Optional).
     * @throws Exception Well this is embarrassing.
     */
    public static void convertConfigurationFile(File inputFile, File nodeCfgFile, File clusterCfgFile,
            boolean includeDefaults, @Nullable ClassLoader clientClassloader) throws Exception {
        MultiValuedMap<Object, Pair<PropertyDescriptor, BeanDefinition>> propertyTracker = new HashSetValuedHashMap<>();
        CustomBeanFactory beanFactory = new CustomBeanFactory(propertyTracker);
        IgniteConfiguration igniteCfg =
                Ignite2ConfigurationUtils.loadIgnite2Configuration(new FileSystemResource(inputFile), beanFactory, clientClassloader);
        igniteCfg = spyIgniteConfiguration(propertyTracker, igniteCfg, IgniteConfiguration.class);

        var registry = Ignite3ConfigurationUtils.loadCombinedRegistry(nodeCfgFile.toPath(), clusterCfgFile.toPath(), includeDefaults);
        registry.onDefaultsPersisted().get(10, TimeUnit.SECONDS);
        LOGGER.info("Finished loading configurations into memory");

        LOGGER.info("Starting to map configurations");
        for (org.apache.ignite.migrationtools.config.converters.ConfigurationConverter converter : converters) {
            converter.convert(igniteCfg, registry);
        }

        // List the remaining properties in the tracker.
        for (Map.Entry<Object, Collection<Pair<PropertyDescriptor, BeanDefinition>>> e : propertyTracker.asMap().entrySet()) {
            List<String> propertyNames = e.getValue().stream().map(p -> p.getKey().getName()).collect(Collectors.toList());
            LOGGER.warn("Could not convert properties: {} - {}", e.getKey().getClass().getName(), propertyNames);
        }

        LOGGER.info("Finished mapping configurations. Waiting until files are written...");
        registry.stopAsync().join();
    }

    private static <T> T spyIgniteConfiguration(
            MultiValuedMap<Object, Pair<PropertyDescriptor, BeanDefinition>> propertyTracker, T obj, Class<T> klass) {
        return Mockito.mock(klass, Mockito.withSettings()
                .spiedInstance(obj)
                .defaultAnswer(ans -> {
                    // Check if the method was registered
                    Method calledMethod = ans.getMethod();
                    propertyTracker.get(obj)
                            .stream()
                            .filter(e -> Objects.equals(e.getLeft().getReadMethod(), calledMethod))
                            .findFirst()
                            .ifPresent(e -> {
                                LOGGER.info("Registered property access: {} - {}::{}", obj.hashCode(), e.getLeft().getReadMethod(),
                                        calledMethod);
                                // We will remove the property from the tracker.
                                propertyTracker.removeMapping(obj, e);
                            });

                    // Spy object recursively
                    Object ret = ans.callRealMethod();
                    if (ret != null && typeMockabilityOf(ret.getClass(), null).mockable()) {
                        return spyIgniteConfiguration(propertyTracker, (T) ret, (Class<T>) ret.getClass());
                    } else {
                        return ret;
                    }
                }));
    }

}
