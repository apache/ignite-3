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

package org.apache.ignite.serialization;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.MalformedJsonException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.serialization.descriptor.TypeDescriptor;

public class JsonObjectSerializer implements UserObjectSerializer {
    private final Gson gson = new GsonBuilder()
            .serializeNulls()
            .setObjectToNumberStrategy(JsonObjectSerializer::readNumber)
            .create();

    @Override
    public <T> byte[] serialize(T object) {
        return gson.toJson(object).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public <T> T deserialize(byte[] bytes, TypeDescriptor descriptor) {
        return gson.fromJson(new String(bytes, StandardCharsets.UTF_8), descriptor.type());
    }

    private static Number readNumber(JsonReader in) throws IOException, JsonParseException {
        String value = in.nextString();

        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException intE) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException longE) {
                try {
                    return Float.valueOf(value);
                } catch (NumberFormatException floatE) {
                    try {
                        Double d = Double.valueOf(value);
                        if ((d.isInfinite() || d.isNaN()) && !in.isLenient()) {
                            throw new MalformedJsonException("JSON forbids NaN and infinities: " + d + "; at path " + in.getPreviousPath());
                        }
                        return d;
                    } catch (NumberFormatException doubleE) {
                        throw new JsonParseException("Cannot parse " + value + "; at path " + in.getPreviousPath(), doubleE);
                    }
                }
            }
        }
    }
}
