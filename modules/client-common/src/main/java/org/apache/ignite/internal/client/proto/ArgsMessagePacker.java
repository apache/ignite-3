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

package org.apache.ignite.internal.client.proto;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.apache.ignite.compute.arg.Args;
import org.apache.ignite.compute.arg.MappedArgs;
import org.apache.ignite.compute.arg.Mapper;
import org.apache.ignite.compute.arg.PojoArgs;

public class ArgsMessagePacker {
    public static void pack(ClientMessagePacker w, Args args) {
        if (args instanceof PojoArgs) {
            w.packNil();
            w.packObjectArrayAsBinaryTuple(args.args());
        } else if (args instanceof MappedArgs) {
            String mapperClassName = ((MappedArgs) args).mapperClassName();
            w.packString(mapperClassName);
            w.packObjectArrayAsBinaryTuple(args.args());
        } else if (args == null) {
            w.packNil();
            w.packNil();
        } else {
            w.packNil();
            w.packObjectArrayAsBinaryTuple(new Object[0]);
        }
    }

    public static Args unpack(ClientMessageUnpacker in, String jobClassName) {
        Mapper mapper;
        try {
            if (in.tryUnpackNil()) {
                mapper = null;
            } else {
                String mapperClassName = in.unpackString();

                Class<? extends Mapper> mapperClass = (Class<? extends Mapper>) Class.forName(mapperClassName);
                Constructor<? extends Mapper> declaredConstructor = mapperClass.getDeclaredConstructor();
                declaredConstructor.setAccessible(true);
                mapper = declaredConstructor.newInstance();
            }
        } catch (ClassNotFoundException
                 | InvocationTargetException
                 | InstantiationException
                 | IllegalAccessException
                 | NoSuchMethodException ce
        ) {
            throw new RuntimeException(ce);
        }

        Object[] args = in.unpackObjectArrayFromBinaryTuple();

        return PojoArgs.fromArray(mapper != null ? mapper.map(args) : args);
    }
}
