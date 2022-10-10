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

package org.apache.ignite.internal.cli.commands.node.metric;

import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.call.node.metric.NodeMetricListCall;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.commands.node.NodeUrlMixin;
import org.apache.ignite.internal.cli.commands.questions.ConnectToClusterQuestion;
import org.apache.ignite.internal.cli.core.call.StringCallInput;
import org.apache.ignite.internal.cli.core.flow.builder.Flows;
import org.apache.ignite.internal.cli.decorators.MetricListDecorator;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

/** Command that lists node metric sources in REPL mode. */
@Command(name = "list", description = "Lists node metric sources")
public class NodeMetricListReplCommand extends BaseCommand implements Runnable {
    /** Node URL option. */
    @Mixin
    private NodeUrlMixin nodeUrl;

    @Inject
    private NodeMetricListCall call;

    @Inject
    private ConnectToClusterQuestion question;

    /** {@inheritDoc} */
    @Override
    public void run() {
        question.askQuestionIfNotConnected(nodeUrl.getNodeUrl())
                .map(StringCallInput::new)
                .then(Flows.fromCall(call))
                .print(new MetricListDecorator())
                .start();
    }
}
