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

package org.apache.ignite.table.mapper;

import java.util.function.Function;
import org.apache.ignite.table.Tuple;

/**
 * Mapper interface defines methods that are required for a marshaller to map class field names to table columns.
 *
 * @param <T> Mapped type.
 */
public interface Mapper<T> {
    /**
     * Return mapped type.
     *
     * @return Mapped type.
     */
    Class<T> targetType();
    
    String fieldForColumn(String name);
    
    /**
     * Creates identity mapper which is used for simple types that have native support or objects with field names that match column names.
     *
     * @param targetClass Target type class.
     * @param <T>         Target type.
     * @return Mapper.
     */
    static <T> Mapper<T> identityMapper(Class<T> targetClass) {
        return new Mapper<>() {
            @Override
            public Class<T> targetType() {
                return targetClass;
            }
            
            @Override
            public String fieldForColumn(String name) {
                try {
                    targetClass.getDeclaredField(name);
                    
                    return name;
                } catch (NoSuchFieldException e) {
                    return null;
                }
            }
        };
    }
    
    /**
     * Mapper builder.
     *
     * @param <T> Mapped type.
     */
    interface Builder<T> {
        /**
         * Map a field to a type of given class.
         *
         * @param fieldName   Field name.
         * @param targetClass Target class.
         * @return {@code this} for chaining.
         */
        Builder<T> map(String fieldName, Class<?> targetClass);
        
        /**
         * Adds a functional mapping for a field, the result depends on function call for every particular row.
         *
         * @param fieldName       Field name.
         * @param mappingFunction Mapper function.
         * @return {@code this} for chaining.
         */
        Builder<T> map(String fieldName, Function<Tuple, Object> mappingFunction);
        
        /**
         * Sets a target class to deserialize to.
         *
         * @param targetClass Target class.
         * @return {@code this} for chaining.
         */
        Builder<T> deserializeTo(Class<?> targetClass);
        
        /**
         * Builds mapper.
         *
         * @return Mapper.
         */
        Mapper<T> build();
    }
}
