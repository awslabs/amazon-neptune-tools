package com.amazonaws.services.neptune.util;

import java.util.HashMap;
import java.util.Map;

public class MapUtils {

    public static Map<?, ?> map(Entry... entries) {
        HashMap<Object, Object> map = new HashMap<>();
        for (Entry entry : entries) {
            map.put(entry.key(), entry.value());
        }
        return map;
    }

    public static Entry entry(String key, Object value) {
        return new Entry(key, value);
    }

    public static class Entry {

        private final String key;
        private final Object value;

        private Entry(String key, Object value) {
            this.key = key;
            this.value = value;
        }

        public String key() {
            return key;
        }

        public Object value() {
            return value;
        }
    }
}
