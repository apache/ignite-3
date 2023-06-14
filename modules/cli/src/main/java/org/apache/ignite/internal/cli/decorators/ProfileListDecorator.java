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

package org.apache.ignite.internal.cli.decorators;

import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.Style;
import static org.apache.ignite.internal.cli.core.style.AnsiStringSupport.ansi;

import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.call.cliconfig.profile.ProfileList;
import org.apache.ignite.internal.cli.core.decorator.Decorator;
import org.apache.ignite.internal.cli.core.decorator.TerminalOutput;

/**
 * Decorator for printing {@link ProfileList}.
 */
public class ProfileListDecorator implements Decorator<ProfileList, TerminalOutput> {
    @Override
    public TerminalOutput decorate(ProfileList data) {
        if (isatty()) {
            return decorateCurrentProfileName(data);
        } else {
            return () -> data.getProfileNames().stream().collect(Collectors.joining(System.lineSeparator()));
        }
    }

    private TerminalOutput decorateCurrentProfileName(ProfileList data) {
        String currentProfileName = data.getCurrentProfileName();
        return () -> data.getProfileNames().stream()
                .map(p -> {
                    if (p.equals(currentProfileName)) {
                        return ansi(Style.BOLD.mark("* " + p));
                    } else {
                        return "  " + p;
                    }
                })
                .collect(Collectors.joining(System.lineSeparator()));
    }

    /**
     * Determine whether the console output is redirected or not.
     *
     * @return {@code true} if output is a terminal
     */
    private static boolean isatty() {
        return System.console() != null;
    }
}
