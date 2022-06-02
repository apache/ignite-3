package org.apache.ignite.cli.commands.decorators;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.cli.commands.decorators.core.Decorator;
import org.apache.ignite.cli.commands.decorators.core.TerminalOutput;

/**
 * Pretty json decorator.
 */
public class JsonDecorator implements Decorator<String, TerminalOutput> {

    /** {@inheritDoc} */
    @Override
    public TerminalOutput decorate(String jsonString) {
        ObjectMapper mapper = new ObjectMapper();
        return () -> {
            try {
                return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.readValue(jsonString, JsonNode.class));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
