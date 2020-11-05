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

package com.amazonaws.services.neptune.propertygraph;

public class NamedQuery {
    private final String name;
    private final String query;

    public NamedQuery(String name, String query) {

        if (query.contains(".addV(") || query.contains(".addE(") || query.contains(".drop(") || query.contains(".property(")){
            throw new IllegalArgumentException("Query must not contain any Gremlin write steps");
        }


        this.name = name;
        this.query = query;


    }

    public String name() {
        return name;
    }

    public String query() {
        return query;
    }
}
