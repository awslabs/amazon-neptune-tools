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

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;
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
    },
    Boolean {
        @Override
        public String typeDescription() {
            return ":bool";
        }

        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeBoolean((boolean) value);
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeBooleanField(key, (boolean) value);
        }

        @Override
        public boolean isNumeric() {
            return false;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Boolean.parseBoolean(java.lang.String.valueOf(value));
        }
    },
    Byte {
        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeNumber((byte) value);
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeNumberField(key, (byte) value);
        }

        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Byte.parseByte(java.lang.String.valueOf(value));
        }
    },
    Short {
        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeNumber((short) value);
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeNumberField(key, (short) value);
        }

        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Short.parseShort(java.lang.String.valueOf(value));
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
    },
    Long {
        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeNumber((long) value);
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeNumberField(key, (long) value);
        }

        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Long.parseLong(java.lang.String.valueOf(value));
        }
    },
    Float {
        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeNumber((float) value);
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeNumberField(key, (float) value);
        }

        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Float.parseFloat(java.lang.String.valueOf(value));
        }
    },
    Double {
        @Override
        public void printTo(JsonGenerator generator, Object value) throws IOException {
            generator.writeNumber((double) value);
        }

        @Override
        public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
            generator.writeNumberField(key, (double) value);
        }

        @Override
        public boolean isNumeric() {
            return true;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.Double.parseDouble(java.lang.String.valueOf(value));
        }
    },
    String {
        @Override
        public String format(Object value) {
            return java.lang.String.format(
                    "\"%s\"",
                    escapeDoubleQuotes(value));
        }

        public String escapeDoubleQuotes(Object value) {
            String temp = value.toString().replace("\"\"", "\"");
            return temp.replace("\"", "\"\"");
        }


        @Override
        public String formatList(Collection<?> values) {
            return java.lang.String.format("\"%s\"",
                    values.stream().
                            map(DataType::escapeSemicolons).
                            map(this::escapeDoubleQuotes).
                            collect(Collectors.joining(";")));
        }

        @Override
        public boolean isNumeric() {
            return false;
        }

        @Override
        public Object convert(Object value) {
            return java.lang.String.valueOf(value);
        }
    },
    Date {
        @Override
        public String format(Object value) {
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
        public boolean isNumeric() {
            return false;
        }

        @Override
        public Object convert(Object value) {
            if (java.util.Date.class.isAssignableFrom(value.getClass())){
                return value;
            }
            Instant instant = Instant.parse(value.toString());
            return new java.util.Date(instant.toEpochMilli());
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

    public static String escapeSemicolons(Object value) {
        String temp = value.toString().replace("\\;", ";");
        return temp.replace(";", "\\;");
    }

    public String typeDescription() {
        return java.lang.String.format(":%s", name().toLowerCase());
    }

    public String format(Object value) {
        return value.toString();
    }

    public void printTo(JsonGenerator generator, Object value) throws IOException {
        generator.writeString(value.toString());
    }

    public void printTo(JsonGenerator generator, String key, Object value) throws IOException {
        generator.writeStringField(key, value.toString());
    }

    public String formatList(Collection<?> values) {
        return values.stream().map(this::format).collect(Collectors.joining(";"));
    }

    public abstract boolean isNumeric();

    public abstract Object convert(Object value);
}
