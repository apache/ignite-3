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

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.ignite.otel.ext.instrumentation.netty.NettySenderSingletons.instrumenter;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.context.Context;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.HashMap;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import org.apache.ignite.network.OutNetworkObject;

/**
 * DemoServlet3Instrumentation.
 */
public class NettySenderInstrumentation implements TypeInstrumentation {
    @Override
    public ElementMatcher<TypeDescription> typeMatcher() {
        return named("org.apache.ignite.internal.network.netty.NettySender");
    }

    @Override
    public void transform(TypeTransformer transformer) {
        transformer.applyAdviceToMethod(
                named("send"),
                this.getClass().getName() + "$SendAdvice");
    }

    @SuppressWarnings("unused")
    public static class SendAdvice {
        @Advice.OnMethodEnter(suppress = Throwable.class)
        public static void onEnter(
                @Advice.Argument(value = 0, readOnly = false) OutNetworkObject obj
        ) {
            System.out.println("onEnter");
            try {
                Context parentContext = Context.current();
                if (!instrumenter().shouldStart(parentContext, obj)) {
                    return;
                }

                var context = instrumenter().start(parentContext, obj);
                var scope = context.makeCurrent();

                System.out.println("makeCurrent");

                // Request is immutable, so we have to assign new value once we update headers
                var propagator = GlobalOpenTelemetry.get().getPropagators().getTextMapPropagator();
                var headers = new HashMap<String, String>(propagator.fields().size());

                propagator.inject(context, headers, MapSetter.INSTANCE);

                obj = new OutNetworkObject(obj.networkMessage(), obj.descriptors(), headers, obj.shouldBeSavedForRecovery());
            } catch (Throwable e) {
                System.out.println(e);

                throw e;
            }
        }
    }
}
