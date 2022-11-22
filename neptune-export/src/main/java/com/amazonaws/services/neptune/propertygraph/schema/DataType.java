/*
Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.propertygraph.schema;

import com.amazonaws.services.neptune.propertygraph.io.CsvPrinterOptions;
import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.stream.Collectors;

public enum DataType {

    None {
        @Override
        public String typeDescription() {
            return "";
        }

        @Override
        public boolean isNumeric() {
            return false;
        }

        @Override
        public Object convert(Object value) {
            return value;
        }

        @Override
        public int compare(Object v1, Object v2) {
            return -1;
        }
    },
    Boolean {
        @Override
        public String typeDescription() {
            return ":bool";
        }

        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeBoolean((java.lang.Boolean) Boolean.convert(value));
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeBooleanField(key, (java.lang.Boolean) Boolean.convert(value));
        }

        @Override
        public boolean isNumeric() {
            return false;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Boolean.parseBoolean(java.lang.String.valueOf(value));
        }

        @Override
        public int compare(Object v1, Object v2) {
            return java.lang.Boolean.compare((boolean) v1, (boolean) v2);
        }
    },
    Byte {
        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeNumber((java.lang.Byte) Byte.convert(value));
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeNumberField(key, (java.lang.Byte) Byte.convert(value));
        }

        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Byte.parseByte(java.lang.String.valueOf(value));
        }

        @Override
        public int compare(Object v1, Object v2) {
            return java.lang.Byte.compare((Byte) v1, (Byte) v2);
        }
    },
    Short {
        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeNumber((java.lang.Short) Short.convert(value));
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeNumberField(key, (java.lang.Short) Short.convert(value));
        }

        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Short.parseShort(java.lang.String.valueOf(value));
        }

        @Override
        public int compare(Object v1, Object v2) {
            return java.lang.Short.compare((short) v1, (short) v2);
        }
    },
    Integer {
        @Override
        public String typeDescription() {
            return ":int";
        }

        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeNumber((int) value);
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeNumberField(key, (int) value);
        }

        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Integer.parseInt(java.lang.String.valueOf(value));
        }

        @Override
        public int compare(Object v1, Object v2) {
            return java.lang.Integer.compare((int) v1, (int) v2);
        }
    },
    Long {
        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeNumber((java.lang.Long) Long.convert(value));
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeNumberField(key, (java.lang.Long) Long.convert(value));
        }

        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Long.parseLong(java.lang.String.valueOf(value));
        }

        @Override
        public int compare(Object v1, Object v2) {
            return java.lang.Long.compare((long) v1, (long) v2);
        }
    },
    Float {
        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeNumber((java.lang.Float) Float.convert(value));
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeNumberField(key, (java.lang.Float) Float.convert(value));
        }

        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Float.parseFloat(java.lang.String.valueOf(value));
        }

        @Override
        public int compare(Object v1, Object v2) {
            return java.lang.Float.compare((float) v1, (float) v2);
        }
    },
    Double {
        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeNumber((java.lang.Double) Double.convert(value));
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeNumberField(key, (java.lang.Double) Double.convert(value));
        }

        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Double.parseDouble(java.lang.String.valueOf(value));
        }

        @Override
        public int compare(Object v1, Object v2) {
            return java.lang.Double.compare((double) v1, (double) v2);
        }
    },
    String {
        @Override
        public String format(Object value) {
            return format(value, false);
        }

        @Override
        public String format(Object value, boolean escapeNewline) {
            java.lang.String escaped = escapeDoubleQuotes(value);
            if (escapeNewline){
                escaped = escapeNewlineChar(escaped);
            }
            if (StringUtils.isNotEmpty(escaped)) {
                return java.lang.String.format("\"%s\"", escaped);
            } else {
                return "";
            }
        }

        private String escapeNewlineChar(String value) {
            return value.replace("\n", "\\n");
        }


        @Override
        public String formatList(Collection<?> values, CsvPrinterOptions options) {
            if (values.isEmpty()) {
                return "";
            }

            return java.lang.String.format("\"%s\"",
                    values.stream().
                            map(v -> DataType.escapeSeparators(v, options.multiValueSeparator())).
                            map(DataType::escapeDoubleQuotes).
                            map(v -> options.escapeNewline() ? escapeNewlineChar(v) : v).
                            collect(Collectors.joining(options.multiValueSeparator())));
        }

        @Override
        public boolean isNumeric() {
            return false;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.String.valueOf(value);
        }

        @Override
        public int compare(Object v1, Object v2) {
            return java.lang.String.valueOf(v1).compareTo(java.lang.String.valueOf(v2));
        }
    },
    Date {
        @Override
        public String format(Object value) {
            return format(value, false);
        }

        @Override
        public String format(Object value, boolean escapeNewline) {
            try {
                java.util.Date date = (java.util.Date) value;
                return DateTimeFormatter.ISO_INSTANT.format(date.toInstant());

            } catch (ClassCastException e) {
                return value.toString();
            }
        }

        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeString(format(value));
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeStringField(key, format(value));
        }

        @Override
        public void printAsStringTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeStringField(key, format(value));
        }

        @Override
        public boolean isNumeric() {
            return false;
        }

        @Override
        public Object convert(Object value) {
            if (java.util.Date.class.isAssignableFrom(value.getClass())) {
                return value;
            }
            Instant instant = Instant.parse(value.toString());
            return new java.util.Date(instant.toEpochMilli());
        }

        @Override
        public int compare(Object v1, Object v2) {
            return ((java.util.Date) v1).compareTo((java.util.Date) v2);
        }
    };

    public static DataType dataTypeFor(Class<?> cls) {
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
        } else if (oldType == Boolean) {
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

    public static String escapeSeparators(Object value, String separator) {
        if (separator.isEmpty()) {
            return value.toString();
        }
        String temp = value.toString().replace("\\" + separator, separator);
        return temp.replace(separator, "\\" + separator);
    }

    public static String escapeDoubleQuotes(Object value) {
        return value.toString().replace("\"", "\"\"");
    }

    public String typeDescription() {
        return java.lang.String.format(":%s", name().toLowerCase());
    }

    public String format(Object value) {
        return value.toString();
    }

    public String format(Object value, boolean escapeNewline) {
        return value.toString();
    }

    public void printTo(JsonGenerator generator, Object value) throws IOException {
        generator.writeString(value.toString());
    }

    public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
        generator.writeStringField(key, value.toString());
    }

    public void printAsStringTo(JsonGenerator generator, String key, Object value) throws IOException {
        generator.writeStringField(key, value.toString());
    }

    public String formatList(Collection<?> values, CsvPrinterOptions options) {
        return values.stream().map(v -> format(v, options.escapeNewline())).collect(Collectors.joining(options.multiValueSeparator()));
    }

    public abstract boolean isNumeric();

    public abstract Object convert(Object value);

    public abstract int compare(Object v1, Object v2);
}
