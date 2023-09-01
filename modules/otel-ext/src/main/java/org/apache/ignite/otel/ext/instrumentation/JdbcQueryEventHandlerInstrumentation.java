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

import static net.bytebuddy.matcher.ElementMatchers.namedOneOf;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;

/**
 * DemoServlet3Instrumentation.
 */
public class JdbcQueryEventHandlerInstrumentation implements TypeInstrumentation {
    /** {@inheritDoc} */
    @Override
    public ElementMatcher<TypeDescription> typeMatcher() {
        return AgentElementMatchers.hasSuperType(
                namedOneOf("org.apache.ignite.client.handler.JdbcQueryEventHandlerImpl"));
    }

    /** {@inheritDoc} */
    @Override
    public void transform(TypeTransformer typeTransformer) {
        typeTransformer.applyAdviceToMethod(
                namedOneOf("queryAsync")
                        .and(ElementMatchers.takesArgument(0, long.class))
                        .and(ElementMatchers.takesArgument(1,
                                ElementMatchers.named("org.apache.ignite.internal.jdbc.proto.event.JdbcQueryExecuteRequest")))
                        .and(ElementMatchers.isPublic()),
                this.getClass().getName() + "$JdbcQueryEventHandlerAdvice");
    }

    /**
     * DemoServlet3Advice.
     */
    @SuppressWarnings("unused")
    public static class JdbcQueryEventHandlerAdvice {

        /**
         * Add some attributes on enter to method.
         *
         * @param connectionId connection id.
         * @param req request.
         */
        @Advice.OnMethodEnter(suppress = Throwable.class)
        public static void onEnter(@Advice.Argument(value = 0) long connectionId, @Advice.Argument(value = 1) Object req) {
            // TODO: 04.09.2023 rewrite using io.opentelemetry.instrumentation.api.instrumenter.Instrumenter
            Span.current()
                    .setAttribute("connectionId", connectionId)
                    .setAttribute("req", req.toString());
        }
    }
}