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

package org.apache.ignite.otel.ext.instrumentation.netty;

import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.hasClassesNamed;
import static java.util.Collections.singletonList;

import com.google.auto.service.AutoService;
import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import java.util.List;
import net.bytebuddy.matcher.ElementMatcher.Junction;

/**
 * This is a demo instrumentation which hooks into servlet invocation and modifies the http response.
 */
@AutoService(InstrumentationModule.class)
public class NettySenderInstrumentationModule extends InstrumentationModule {
    public NettySenderInstrumentationModule() {
        super("netty", "netty-4.1");
    }

    /**
     * The instrumentation to be applied after the standard method instrumentation.
     * The latter creates a server span around. This instrumentation needs access to that server span.
     */
    @Override
    public int order() {
        return 1;
    }

    @Override
    public Junction<ClassLoader> classLoaderMatcher() {
        return hasClassesNamed("org.apache.ignite.internal.network.netty.NettySender");
    }

    @Override
    public List<TypeInstrumentation> typeInstrumentations() {
        return singletonList(new NettySenderInstrumentation());
    }

    @Override
    public List<String> getAdditionalHelperClassNames() {
        return singletonList(NettySenderSingletons.class.getName());
    }
}
