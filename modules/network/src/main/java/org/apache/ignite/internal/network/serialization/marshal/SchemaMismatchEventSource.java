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

/**
 * An object on which {@link SchemaMismatchHandler}s may be registered.
 */
public interface SchemaMismatchEventSource {
    /**
     * Sets the {@link SchemaMismatchHandler} for the given class if not set or replaces the existing one.
     *
     * <p>Note that the handlers are per declared class, not per concrete class lineage.
     *
     * @param layerClass the class; for schema changes concerning this class the events will be generated
     * @param handler    the handler that will handle the schema mismatch events
     * @param <T>        layer type
     */
    <T> void replaceSchemaMismatchHandler(Class<T> layerClass, SchemaMismatchHandler<T> handler);

    /**
     * Sets the {@link SchemaMismatchHandler} for the given class if not set or replaces the existing one.
     *
     * <p>Note that the handlers are per declared class, not per concrete class lineage.
     *
     * @param layerClassName the name of the class; for schema changes concerning this class the events will be generated
     * @param handler    the handler that will handle the schema mismatch events
     * @param <T>        layer type
     */
    <T> void replaceSchemaMismatchHandler(String layerClassName, SchemaMismatchHandler<T> handler);
}
