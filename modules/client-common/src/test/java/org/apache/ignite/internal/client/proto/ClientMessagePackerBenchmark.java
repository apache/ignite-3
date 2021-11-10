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

package org.apache.ignite.internal.client.proto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Message packer benchmark.
 */
@State(Scope.Benchmark)
public class ClientMessagePackerBenchmark {
    private static final ByteBuf buffer = Unpooled.buffer(1024);
    
    @Benchmark
    public void packString() {
        var packer = new ClientMessagePacker(buffer.writerIndex(0));
        
        packer.packString("The quick brown fox jumps over the lazy dog.");
    }
    
    @Benchmark
    public void packInt() {
        var packer = new ClientMessagePacker(buffer.writerIndex(0));
        
        packer.packInt(10);
        packer.packInt(Integer.MAX_VALUE);
    }
    
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ClientMessagePackerBenchmark.class.getSimpleName())
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(10))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(10))
                .forks(1)
                .build();
        
        new Runner(opt).run();
    }
}
