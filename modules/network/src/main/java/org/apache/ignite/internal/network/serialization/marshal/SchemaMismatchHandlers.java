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

package org.apache.ignite.internal.network.serialization.marshal;

import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * A colleciton of {@link SchemaMismatchHandler}s keyed by classes to which they relate.
 */
class SchemaMismatchHandlers {
    private final Map<String, SchemaMismatchHandler<?>> handlers = new HashMap<>();
    private final SchemaMismatchHandler<Object> defaultHandler = new DefaultSchemaMismatchHandler();

    <T> void registerHandler(Class<T> layerClass, SchemaMismatchHandler<T> handler) {
        registerHandler(layerClass.getName(), handler);
    }

    <T> void registerHandler(String layerClassName, SchemaMismatchHandler<T> handler) {
        handlers.put(layerClassName, handler);
    }

    private SchemaMismatchHandler<Object> handlerFor(Class<?> clazz) {
        return handlerFor(clazz.getName());
    }

    @SuppressWarnings("unchecked")
    private SchemaMismatchHandler<Object> handlerFor(String className) {
        SchemaMismatchHandler<Object> handler = (SchemaMismatchHandler<Object>) handlers.get(className);
        if (handler == null) {
            handler = defaultHandler;
        }
        return handler;
    }

    void onFieldIgnored(String layerClassName, Object instance, String fieldName, Object fieldValue) throws SchemaMismatchException {
        handlerFor(layerClassName).onFieldIgnored(instance, fieldName, fieldValue);
    }

    void onFieldMissed(String layerClassName, Object instance, String fieldName) throws SchemaMismatchException {
        handlerFor(layerClassName).onFieldMissed(instance, fieldName);
    }

    void onFieldTypeChanged(String layerClassName, Object instance, String fieldName, Class<?> remoteType, Object fieldValue)
            throws SchemaMismatchException {
        handlerFor(layerClassName).onFieldTypeChanged(instance, fieldName, remoteType, fieldValue);
    }

    void onExternalizableIgnored(Object instance, ObjectInput externalData) throws SchemaMismatchException {
        handlerFor(instance.getClass()).onExternalizableIgnored(instance, externalData);
    }

    void onExternalizableMissed(Object instance) throws SchemaMismatchException {
        handlerFor(instance.getClass()).onExternalizableMissed(instance);
    }

    boolean onReadResolveAppeared(Object instance) throws SchemaMismatchException {
        return handlerFor(instance.getClass()).onReadResolveAppeared(instance);
    }

    void onReadResolveDisappeared(Object instance) throws SchemaMismatchException {
        handlerFor(instance.getClass()).onReadResolveDisappeared(instance);
    }

    void onReadObjectIgnored(String layerClassName, Object instance, ObjectInputStream objectData) throws SchemaMismatchException {
        handlerFor(layerClassName).onReadObjectIgnored(instance, objectData);
    }

    void onReadObjectMissed(String layerClassName, Object instance) throws SchemaMismatchException {
        handlerFor(layerClassName).onReadObjectMissed(instance);
    }
}
