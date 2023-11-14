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

package org.apache.ignite.internal.rest.problem;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.core.io.buffer.ByteBuffer;
import io.micronaut.core.io.buffer.ByteBufferFactory;
import io.micronaut.core.type.Argument;
import io.micronaut.http.MediaType;
import io.micronaut.http.codec.CodecException;
import io.micronaut.http.codec.MediaTypeCodec;
import jakarta.inject.Singleton;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;

/**
 * Problem json media type codec.
 */
@Singleton
public class ProblemJsonMediaTypeCodec implements MediaTypeCodec {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Collection<MediaType> getMediaTypes() {
        return List.of(ProblemJsonMediaType.APPLICATION_JSON_PROBLEM_TYPE);
    }

    @Override
    public <T> T decode(Argument<T> type, InputStream inputStream) throws CodecException {
        try {
            return mapper.readValue(inputStream, type.getType());
        } catch (Exception e) {
            throw new CodecException("Failed to decode input stream", e);
        }
    }

    @Override
    public <T> void encode(T object, OutputStream outputStream) throws CodecException {
        try {
            mapper.writeValue(outputStream, object);
        } catch (Exception e) {
            throw new CodecException("Failed to encode output stream", e);
        }
    }

    @Override
    public <T> byte[] encode(T object) throws CodecException {
        try {
            return mapper.writeValueAsBytes(object);
        } catch (Exception e) {
            throw new CodecException("Failed to encode output stream", e);
        }
    }

    @Override
    public <T, B> ByteBuffer<B> encode(T object, ByteBufferFactory<?, B> allocator) throws CodecException {
        try {
            return allocator.wrap(mapper.writeValueAsBytes(object));
        } catch (Exception e) {
            throw new CodecException("Failed to encode output stream", e);
        }
    }
}
