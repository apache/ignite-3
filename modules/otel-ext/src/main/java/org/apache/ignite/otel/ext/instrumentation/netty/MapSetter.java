package org.apache.ignite.otel.ext.instrumentation.netty;

import io.opentelemetry.context.propagation.TextMapSetter;
import java.util.Map;

enum MapSetter implements TextMapSetter<Map<String, String>> {
    INSTANCE;

    @Override
    public void set(Map<String, String> carrier, String key, String val) {
        carrier.put(key, val);
    }
}
