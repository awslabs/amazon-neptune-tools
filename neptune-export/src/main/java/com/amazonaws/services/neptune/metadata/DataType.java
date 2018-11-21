package com.amazonaws.services.neptune.metadata;

import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.stream.Collectors;

public enum DataType {

    None {
        @Override
        public String typeDescription() {
            return "";
        }
    },
    Boolean {
        @Override
        public String typeDescription() {
            return ":bool";
        }
    },
    Byte,
    Short,
    Integer {
        @Override
        public String typeDescription() {
            return ":int";
        }
    },
    Long,
    Float,
    Double,
    String {
        @Override
        public String format(Object value) {
            return java.lang.String.format(
                    "\"%s\"",
                    doubleQuotes(value));
        }

        private String doubleQuotes(Object value) {
            return value.toString().replace("\"", "\"\"");
        }

        @Override
        public String formatList(Collection<?> values) {
            return java.lang.String.format("\"%s\"",
                    values.stream().
                            map(this::doubleQuotes).
                            collect(Collectors.joining(";")));
        }
    },
    Date {
        @Override
        public String format(Object value) {
            java.util.Date date = (java.util.Date) value;
            return DateTimeFormatter.ISO_INSTANT.format(date.toInstant());
        }
    };

    public static DataType dataTypeFor(Class cls) {
        String name = cls.getSimpleName();
        try {
            return DataType.valueOf(name);
        } catch (IllegalArgumentException e) {
            return DataType.String;
        }
    }

    public static DataType getBroadestType(DataType oldType, DataType newType) {
        if (oldType == newType) {
            return newType;
        } else if (oldType == None) {
            return newType;
        } else if (oldType == Boolean){
            return String;
        } else if (oldType == String || newType == String) {
            return String;
        } else {
            if (newType.ordinal() > oldType.ordinal()) {
                return newType;
            } else {
                return oldType;
            }
        }
    }

    public String typeDescription() {
        return java.lang.String.format(":%s", name().toLowerCase());
    }

    public String format(Object value) {
        return value.toString();
    }

    public String formatList(Collection<?> values) {
        return values.stream().map(this::format).collect(Collectors.joining(";"));
    }
}
