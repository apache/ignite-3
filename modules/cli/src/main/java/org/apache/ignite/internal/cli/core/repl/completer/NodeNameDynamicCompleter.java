package org.apache.ignite.internal.cli.core.repl.completer;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cli.NodeNameRegistry;

public class NodeNameDynamicCompleter implements DynamicCompleter {

    /** Words, after those the completer should have been activated. */
    private final Set<String> activationPostfixes;

    private final List<String> nodeNames;

    public NodeNameDynamicCompleter(Set<String> activationPostfixes, List<String> nodeNames) {
        this.activationPostfixes = activationPostfixes;
        this.nodeNames = nodeNames;
    }

    @Override
    public List<String> complete(String[] words) {
        final String lastWord = beforeLastNotEmptyWord(0, words);
        final String beforeLastWord = beforeLastNotEmptyWord(1, words);

        if (activationPostfixes.contains(lastWord)) {
            return nodeNames;
        } else if (activationPostfixes.contains(beforeLastWord)) {
            return nodeNames.stream()
                    .filter(it -> it.startsWith(lastWord))
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

    private String beforeLastNotEmptyWord(int index, String[] words) {
        for (int i = words.length - 1 - index; i >= 0; i--) {
            if (!words[i].isEmpty()) {
                return words[i];
            }
        }
        return "";
    }
}
