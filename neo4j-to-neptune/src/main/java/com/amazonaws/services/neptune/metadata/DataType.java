/*
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.metadata;

import org.apache.commons.lang3.StringUtils;

import java.util.regex.Pattern;

public enum DataType {
    None {
        @Override
        public String typeDescription() {
            return "";
        }
    },
    Boolean,
    Byte {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    Short {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    Int {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    Long {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    Double {
        @Override
        public boolean isNumeric() {
            return true;
        }
    },
    Date,
    String;

    public boolean isNumeric() {
        return false;
    }

    public String typeDescription() {
        return java.lang.String.format(":%s", name().toLowerCase());
    }

    public static DataType identifyType(String s) {

        if (isBoolean(s)) {
            return DataType.Boolean;
        } else if (isByte(s)) {
            return DataType.Byte;
        } else if (isShort(s)) {
            return DataType.Short;
        } else if (isInt(s)) {
            return DataType.Int;
        } else if (isLong(s)) {
            return DataType.Long;
        } else if (isDouble(s)) {
            return DataType.Double;
        } else {
            if (StringUtils.isEmpty(s)) {
                return DataType.None;
            }
            try {
                DateTimeUtils.parseISODate(s);
                return DataType.Date;
            } catch (Exception e) {
                return DataType.String;
            }
        }
    }

    private static final String BOOLEAN_PATTERN = "true|false";
    private static final Pattern boolPattern = Pattern.compile(BOOLEAN_PATTERN, Pattern.CASE_INSENSITIVE);


    private static boolean isBoolean(String s) {
        return boolPattern.matcher(s).matches();
    }

    private static boolean isByte(String s) {
        try {
            java.lang.Byte.parseByte(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isShort(String s) {
        try {
            java.lang.Short.parseShort(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isInt(String s) {
        try {
            java.lang.Integer.parseInt(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isLong(String s) {
        try {
            java.lang.Long.parseLong(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static boolean isDouble(String s) {
        try {
            java.lang.Double.parseDouble(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public static DataType getBroadestType(DataType oldType, DataType newType) {

        if (oldType == newType) {
            return oldType;
        }

        if (oldType == DataType.None) {
            return newType;
        }

        if (newType == DataType.None) {
            return oldType;
        }

        if (oldType == DataType.String || newType == DataType.String) {
            return DataType.String;
        }

        if (oldType.isNumeric() && newType.isNumeric()) {
            if (newType.ordinal() > oldType.ordinal()) {
                return newType;
            } else {
                return oldType;
            }
        }

        if (newType.ordinal() > oldType.ordinal()) {
            return DataType.String;
        }

        return DataType.String;
    }

}
