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

package org.apache.ignite.internal.cli.commands;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.notNullValue;

import io.micronaut.configuration.picocli.MicronautFactory;
import io.micronaut.context.ApplicationContext;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;
import picocli.CommandLine.Model.OptionSpec;

/**
 * Test for the --profile option.
 */
@MicronautTest
class ProfileOptionTest {
    @Inject
    protected ApplicationContext context;

    @BeforeAll
    static void setDumbTerminal() {
        System.setProperty("org.jline.terminal.dumb", "true");
    }

    @ParameterizedTest
    @ValueSource(classes = {TopLevelCliCommand.class, TopLevelCliReplCommand.class})
    void everyCommandWithUrlOptionHasProfileOption(Class<?> cmdClass) {
        CommandLine cmd = new CommandLine(cmdClass, new MicronautFactory(context));
        assertThat(subCommands(cmd), everyItem(profileOption(notNullValue(OptionSpec.class))));
    }

    private static Matcher<CommandLine> profileOption(Matcher<OptionSpec> optionMatcher) {
        return new TypeSafeMatcher<>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("a command with --profile option ")
                        .appendDescriptionOf(optionMatcher);
            }

            @Override
            protected boolean matchesSafely(CommandLine item) {
                return optionMatcher.matches(item.getCommandSpec().findOption("--profile"));
            }

            @Override
            protected void describeMismatchSafely(CommandLine item, Description mismatchDescription) {
                mismatchDescription.appendValue(item.getCommandSpec().qualifiedName())
                        .appendText(" has --profile option ").appendValue(item.getCommandSpec().findOption("--profile"));
            }
        };
    }

    private static List<CommandLine> subCommands(CommandLine cmd) {
        List<CommandLine> result = new ArrayList<>();

        cmd.getCommandSpec().subcommands().values().forEach(subCmd -> {
            if (subCmd.getCommandSpec().findOption("--url") != null) {
                result.add(subCmd);
            }
            result.addAll(subCommands(subCmd));
        });

        return result;
    }
}
