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

package org.apache.ignite.migrationtools.config.loader;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.CannotLoadBeanClassException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;

/**
 * This factory overrides the BeanWrapper creation.
 */
public class CustomBeanFactory extends DefaultListableBeanFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomBeanFactory.class);

    private static final Constructor<?> NULL_BEAN_CONSTRUCTOR;

    static {
        try {
            Class<?> nullBeanClass = Class.forName("org.springframework.beans.factory.support.NullBean");
            NULL_BEAN_CONSTRUCTOR = nullBeanClass.getDeclaredConstructor();
            NULL_BEAN_CONSTRUCTOR.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private final MultiValuedMap<Object, Pair<PropertyDescriptor, BeanDefinition>> propertyTracker;

    public CustomBeanFactory(MultiValuedMap<Object, Pair<PropertyDescriptor, BeanDefinition>> tracker) {
        this.propertyTracker = tracker;
    }

    @Override
    protected BeanWrapper instantiateBean(String beanName, RootBeanDefinition mbd) {
        BeanWrapper wrapper = super.instantiateBean(beanName, mbd);
        // Might be nullable
        return (wrapper != null) ? new BeanWrapperSpy(this.propertyTracker, wrapper, mbd) : null;
    }

    @Override
    protected Class<?> determineTargetType(String beanName, RootBeanDefinition mbd, Class<?>... typesToMatch) {
        try {
            return super.determineTargetType(beanName, mbd, typesToMatch);
        } catch (CannotLoadBeanClassException ex) {
            // This should be effectively NullBean, but it is package private.
            LOGGER.warn("Could determineTargetType for bean {}; skipping...", beanName, ex);
            return null;
        }
    }

    @Override
    protected Object createBean(String beanName, RootBeanDefinition mbd, Object[] args) throws BeanCreationException {
        // This will make sure that we ship components that are not in the classPath
        try {
            return super.createBean(beanName, mbd, args);
        } catch (CannotLoadBeanClassException | BeanCreationException ex) {
            LOGGER.warn("Could not create bean {}; skipping...", beanName, ex);
            try {
                return NULL_BEAN_CONSTRUCTOR.newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                // this can still allow the program to load
                LOGGER.error("Could not create NULL BEAN", e);
                return null;
            }
        }
    }
}
