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

import java.util.Scanner;

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

        Scanner scanner = new Scanner(s);

        if (scanner.hasNextBoolean()) {
            return DataType.Boolean;
        } else if (scanner.hasNextByte()) {
            return DataType.Byte;
        } else if (scanner.hasNextShort()) {
            return DataType.Short;
        } else if (scanner.hasNextInt()) {
            return DataType.Int;
        } else if (scanner.hasNextLong()) {
            return DataType.Long;
        } else if (scanner.hasNextDouble()) {
            return DataType.Double;
        } else if (scanner.hasNext()){
            java.lang.String v = scanner.next();
            try {
                DateTimeUtils.parseISODate(v);
                return DataType.Date;
            } catch (Exception e) {
                return DataType.String;
            }
        } else {
            return DataType.None;
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
