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

import java.beans.PropertyDescriptor;
import java.io.File;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.HashSetValuedHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.migrationtools.config.loader.CustomBeanFactory;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

/**
 * Utility methods for managing Ignite 2 Configurations.
 */
public class Ignite2ConfigurationUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(Ignite2ConfigurationUtils.class);

    private Ignite2ConfigurationUtils() {
        // Intentionally left blank
    }

    public static IgniteConfiguration loadIgnite2Configuration(File inputFile, boolean skipUnloadableBeans) {
        return loadIgnite2Configuration(new FileSystemResource(inputFile), createBeanFactory(skipUnloadableBeans), null);
    }

    public static IgniteConfiguration loadIgnite2Configuration(File inputFile, boolean skipUnloadableBeans,
            @Nullable ClassLoader clientClassLoader) {
        return loadIgnite2Configuration(new FileSystemResource(inputFile), createBeanFactory(skipUnloadableBeans), clientClassLoader);
    }

    /**
     * Load and Apache Ignite 2 configuration into memory.
     *
     * @param rsrc The input xml configuration file as a resource.
     * @param factory The BeanFactory
     * @return The IgniteConfiguration object.
     */
    public static IgniteConfiguration loadIgnite2Configuration(
            Resource rsrc,
            DefaultListableBeanFactory factory,
            @Nullable ClassLoader classLoader
    ) throws BeansException, IllegalStateException {

        // Get the Spring Bean Def into memory
        GenericApplicationContext springCtx = new GenericApplicationContext(factory);
        springCtx.setClassLoader(classLoader);
        XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(springCtx);

        reader.setValidationMode(XmlBeanDefinitionReader.VALIDATION_XSD);

        var n = reader.loadBeanDefinitions(rsrc);
        LOGGER.info("Loaded Spring definition into memory: {}", n);

        springCtx.refresh();

        return factory.getBean(IgniteConfiguration.class);
    }

    /**
     * Create spring bean factory based on the skipUnloadableBeans property.
     *
     * @param skipUnloadableBeans Whether the factory should skip unloadable beans or fail with an error.
     * @return The appropriate bean factory.
     */
    public static DefaultListableBeanFactory createBeanFactory(boolean skipUnloadableBeans) {
        if (skipUnloadableBeans) {
            MultiValuedMap<Object, Pair<PropertyDescriptor, BeanDefinition>> propertyTracker = new HashSetValuedHashMap<>();
            return new CustomBeanFactory(propertyTracker);
        } else {
            return new DefaultListableBeanFactory();
        }
    }
}
