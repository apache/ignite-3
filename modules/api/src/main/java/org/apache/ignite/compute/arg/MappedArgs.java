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

package org.apache.ignite.compute.arg;


import java.lang.reflect.Constructor;

public class MappedArgs implements Args {
    private final String mapperClassName;

    private final byte[][] args;

    public MappedArgs(String mapperClass, byte[][] args) {
        this.mapperClassName = mapperClass;
        this.args = args;
    }

    public String mapperClassName() {
        return mapperClassName;
    }

    public static MappedArgs fromArray(Class<? extends Mapper> mapperClass, byte[]... args) {
        return new MappedArgs(mapperClass.getName(), args);
    }

    @Override
    public Object[] args() {
        try {
            Class<?> mapperClass = Class.forName(mapperClassName);

            Constructor<?> declaredConstructor = mapperClass.getDeclaredConstructor();
            declaredConstructor.setAccessible(true);
            Object o = declaredConstructor.newInstance();

            if (!(o instanceof Mapper)) {
                throw new IllegalArgumentException();
            }

            Mapper mapper = (Mapper) o;

            return mapper.map(args);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
