/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.config.loader;

import java.beans.PropertyDescriptor;
import java.beans.PropertyEditor;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeansException;
import org.springframework.beans.InvalidPropertyException;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.PropertyValues;
import org.springframework.beans.TypeMismatchException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.MethodParameter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.lang.Nullable;

/**
 * A custom implementation that tracks which properties where explicitly set in the bean.
 */
public class BeanWrapperSpy implements BeanWrapper {
    private final MultiValuedMap<Object, Pair<PropertyDescriptor, BeanDefinition>> propertyTracker;

    private final BeanWrapper base;

    private final BeanDefinition mdb;

    /**
     * Constructor.
     *
     * @param propertyTracker Map that tracks
     * @param base Delegated BeanWrapper
     * @param mbd Root definition
     */
    public BeanWrapperSpy(MultiValuedMap<Object, Pair<PropertyDescriptor, BeanDefinition>> propertyTracker, BeanWrapper base,
            RootBeanDefinition mbd) {
        this.propertyTracker = propertyTracker;
        this.base = base;
        this.mdb = mbd;
    }

    private void registerProperty(PropertyValue pv) {
        Object instance = getWrappedInstance();
        PropertyDescriptor descriptor = getPropertyDescriptor(pv.getName());
        this.propertyTracker.put(instance, Pair.of(descriptor, this.mdb));
    }

    // Delegated methods.

    @Override
    public int getAutoGrowCollectionLimit() {
        return base.getAutoGrowCollectionLimit();
    }

    @Override
    public void setAutoGrowCollectionLimit(int autoGrowCollectionLimit) {
        base.setAutoGrowCollectionLimit(autoGrowCollectionLimit);
    }

    @Override
    public Object getWrappedInstance() {
        return base.getWrappedInstance();
    }

    @Override
    public Class<?> getWrappedClass() {
        return base.getWrappedClass();
    }

    @Override
    public PropertyDescriptor[] getPropertyDescriptors() {
        return base.getPropertyDescriptors();
    }

    @Override
    public PropertyDescriptor getPropertyDescriptor(String propertyName) throws InvalidPropertyException {
        return base.getPropertyDescriptor(propertyName);
    }

    @Override
    @Nullable
    public ConversionService getConversionService() {
        return base.getConversionService();
    }

    @Override
    public void setConversionService(ConversionService conversionService) {
        base.setConversionService(conversionService);
    }

    @Override
    public boolean isExtractOldValueForEditor() {
        return base.isExtractOldValueForEditor();
    }

    @Override
    public void setExtractOldValueForEditor(boolean extractOldValueForEditor) {
        base.setExtractOldValueForEditor(extractOldValueForEditor);
    }

    @Override
    public boolean isAutoGrowNestedPaths() {
        return base.isAutoGrowNestedPaths();
    }

    @Override
    public void setAutoGrowNestedPaths(boolean autoGrowNestedPaths) {
        base.setAutoGrowNestedPaths(autoGrowNestedPaths);
    }

    @Override
    public boolean isReadableProperty(String propertyName) {
        return base.isReadableProperty(propertyName);
    }

    @Override
    public boolean isWritableProperty(String propertyName) {
        return base.isWritableProperty(propertyName);
    }

    @Override
    @Nullable
    public Class<?> getPropertyType(String propertyName) throws BeansException {
        return base.getPropertyType(propertyName);
    }

    @Override
    @Nullable
    public TypeDescriptor getPropertyTypeDescriptor(String propertyName) throws BeansException {
        return base.getPropertyTypeDescriptor(propertyName);
    }

    @Override
    @Nullable
    public Object getPropertyValue(String propertyName) throws BeansException {
        return base.getPropertyValue(propertyName);
    }

    @Override
    public void setPropertyValue(String propertyName, Object value) throws BeansException {
        base.setPropertyValue(propertyName, value);
    }

    @Override
    public void setPropertyValue(PropertyValue pv) throws BeansException {
        base.setPropertyValue(pv);
        registerProperty(pv);
    }

    @Override
    public void setPropertyValues(Map<?, ?> map) throws BeansException {
        base.setPropertyValues(map);
    }

    @Override
    public void setPropertyValues(PropertyValues pvs) throws BeansException {
        base.setPropertyValues(pvs);
        for (PropertyValue pv : pvs) {
            registerProperty(pv);
        }
    }

    @Override
    public void setPropertyValues(PropertyValues pvs, boolean ignoreUnknown) throws BeansException {
        base.setPropertyValues(pvs, ignoreUnknown);
    }

    @Override
    public void setPropertyValues(PropertyValues pvs, boolean ignoreUnknown, boolean ignoreInvalid) throws BeansException {
        base.setPropertyValues(pvs, ignoreUnknown, ignoreInvalid);
    }

    @Override
    public void registerCustomEditor(Class<?> requiredType, PropertyEditor propertyEditor) {
        base.registerCustomEditor(requiredType, propertyEditor);
    }

    @Override
    public void registerCustomEditor(Class<?> requiredType, String propertyPath, PropertyEditor propertyEditor) {
        base.registerCustomEditor(requiredType, propertyPath, propertyEditor);
    }

    @Override
    @Nullable
    public PropertyEditor findCustomEditor(Class<?> requiredType, String propertyPath) {
        return base.findCustomEditor(requiredType, propertyPath);
    }

    @Override
    @Nullable
    public <T> T convertIfNecessary(Object value, Class<T> requiredType) throws TypeMismatchException {
        return base.convertIfNecessary(value, requiredType);
    }

    @Override
    @Nullable
    public <T> T convertIfNecessary(Object value, Class<T> requiredType, MethodParameter methodParam) throws TypeMismatchException {
        // This is some crazy stuff.
        // Since we allow nulls on beans that we cannot create, we might end up with nulls in primitive collections.
        // The solution is to strip them from the source collection.
        // TODO: This should not have been here. But I didn't want to create yet another decorator.
        Object newValue = value;
        if (requiredType.isArray() && requiredType.getComponentType().isPrimitive()) {
            if (value instanceof Collection) {
                newValue = ((Collection<?>) value).stream().filter(Objects::nonNull).toArray();
            } else if (value.getClass().isArray() && !value.getClass().isPrimitive()) {
                newValue = Arrays.stream((Object[]) value).filter(Objects::nonNull).toArray();
            }
        }

        return base.convertIfNecessary(newValue, requiredType, methodParam);
    }

    @Override
    @Nullable
    public <T> T convertIfNecessary(Object value, Class<T> requiredType, Field field) throws TypeMismatchException {
        return base.convertIfNecessary(value, requiredType, field);
    }

    @Override
    @Nullable
    public <T> T convertIfNecessary(Object value, Class<T> requiredType, TypeDescriptor typeDescriptor) throws TypeMismatchException {
        return base.convertIfNecessary(value, requiredType, typeDescriptor);
    }
}
