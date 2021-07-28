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

package com.amazonaws.services.neptune.profiles.neptune_ml.v2.config;

import java.util.ArrayList;
import java.util.Collection;

public enum ImputerTypeV2 {
    mean,
    median,
    most_frequent{
        @Override
        public String formattedName() {
            return "most-frequent";
        }
    },
    none{
        @Override
        public boolean isPublic(){
            return false;
        }
    };


    public String formattedName() {
        return name();
    }

    public boolean isPublic() {
        return true;
    }

    @Override
    public String toString() {
        return formattedName();
    }

    public static ImputerTypeV2 fromString(String s) {
        for (ImputerTypeV2 imputerType : ImputerTypeV2.values()) {
            if (imputerType.formattedName().equals(s)) {
                return imputerType;
            }
        }
        throw new IllegalArgumentException(String.format("Invalid imputer type: %s (valid types are: %s)",
                s,
                String.join(", ", publicFormattedNames())));
    }

    public static Collection<String> publicFormattedNames() {
        Collection<String> results = new ArrayList<>();
        for (ImputerTypeV2 imputerType : ImputerTypeV2.values()) {
            if (imputerType.isPublic()){
                results.add(imputerType.formattedName());
            }
        }
        return results;
    }
}
