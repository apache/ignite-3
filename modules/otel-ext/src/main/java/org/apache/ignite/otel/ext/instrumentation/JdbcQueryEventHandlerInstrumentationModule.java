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

package org.apache.ignite.otel.ext.instrumentation;

import static java.util.Collections.singletonList;

import com.google.auto.service.AutoService;
import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers;
import java.util.List;
import net.bytebuddy.matcher.ElementMatcher;

/**
 * This is a demo instrumentation which hooks into servlet invocation and modifies the http response.
 */
@AutoService(InstrumentationModule.class)
public final class JdbcQueryEventHandlerInstrumentationModule extends InstrumentationModule {
    public JdbcQueryEventHandlerInstrumentationModule() {
        super("ignite3-demo", "jdbc-handler");
    }

    /**
     * The instrumentation to be applied after the standard method instrumentation.
     * The latter creates a server span around. This instrumentation needs access to that server span.
     */
    @Override
    public int order() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override
    public ElementMatcher.Junction<ClassLoader> classLoaderMatcher() {
        return AgentElementMatchers.hasClassesNamed("org.apache.ignite.client.handler.JdbcQueryEventHandlerImpl");
    }

    /** {@inheritDoc} */
    @Override
    public List<TypeInstrumentation> typeInstrumentations() {
        return singletonList(new JdbcQueryEventHandlerInstrumentation());
    }
}