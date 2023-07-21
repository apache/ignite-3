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

package org.apache.ignite.internal.cli.call.connect;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import javax.net.ssl.SSLException;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.flow.Flowable;
import org.apache.ignite.internal.cli.core.flow.builder.FlowBuilder;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * Call which tries to connect to the Ignite 3 node and in case of SSL error asks the user for the SSL configuration and tries again.
 */
@Singleton
public class ConnectSslCall implements Call<ConnectCallInput, String> {
    @Inject
    private ConnectCall connectCall;

    @Inject
    private ConnectSslConfigCall connectSslConfigCall;

    @Override
    public CallOutput<String> execute(ConnectCallInput input) {
        CallOutput<String> output = connectCall.execute(input);
        if (output.hasError()) {
            if (output.errorCause().getCause() instanceof ApiException) {
                ApiException cause = (ApiException) output.errorCause().getCause();
                Throwable apiCause = cause.getCause();
                if (apiCause instanceof SSLException) {
                    FlowBuilder<Void, SslConfig> flowBuilder = ConnectToClusterQuestion.askQuestionOnSslError();
                    Flowable<SslConfig> result = flowBuilder.build().start(Flowable.empty());
                    if (result.hasResult()) {
                        return connectSslConfigCall.execute(new ConnectSslConfigCallInput(input.url(), result.value()));
                    }
                }
            }
        }
        return output;
    }
}
