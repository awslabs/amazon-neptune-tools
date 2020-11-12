/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.profiles.neptune_ml.parsing;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

public enum Norm {
    none{
        @Override
        public String formattedName() {
            return "none";
        }

        @Override
        public void addTo(JsonGenerator generator) {
            // Do nothing
        }
    }
    ,
    min_max{
        @Override
        public String formattedName() {
            return "min-max";
        }

        @Override
        public void addTo(JsonGenerator generator) throws IOException {
            generator.writeStringField("norm", formattedName());
        }
    },
    standard{
        @Override
        public String formattedName() {
            return "standard";
        }

        @Override
        public void addTo(JsonGenerator generator) throws IOException {
            generator.writeStringField("norm", formattedName());
        }
    };

    public abstract String formattedName();

    public abstract void addTo(JsonGenerator generator) throws IOException;

    @Override
    public String toString() {
        return formattedName();
    }

    public static boolean isValid(String s){
        for (Norm value : Norm.values()) {
            if (value.formattedName().equals(s)){
                return true;
            }
        }
        return false;
    }

    public static Norm fromString(String s){
        for (Norm value : Norm.values()) {
            if (value.formattedName().equals(s)){
                return value;
            }
        }
        throw new IllegalArgumentException("Invalid norm value: " + s);
    }

}
