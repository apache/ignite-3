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

package org.apache.ignite.example.serialization;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.marshalling.Marshaller;

public class CustomPojoSerializationJob implements ComputeJob<JsonArg, JsonResult> {
    @Override
    public CompletableFuture<JsonResult> executeAsync(JobExecutionContext ctx, JsonArg arg) {

        if (arg == null) {
            return null;
        }

        String w = arg.word;
        boolean upper = arg.isUpperCase;
        JsonResult r = new JsonResult();
        r.originalWord = w;
        r.resultWord = upper ? w.toUpperCase() : w.toLowerCase();
        r.length = w.length();
        return completedFuture(r);
    }


    private static class JsonArgServerMarshaller extends JsonArgMarshaller {
    }

    private static class JsonResultServerMarshaller extends JsonResultMarshaller {
    }

    @Override
    public Marshaller<JsonArg, byte[]> inputMarshaller() {
        return new JsonArgServerMarshaller();
    }

    @Override
    public Marshaller<JsonResult, byte[]> resultMarshaller() {
        return new JsonResultServerMarshaller();
    }
}
