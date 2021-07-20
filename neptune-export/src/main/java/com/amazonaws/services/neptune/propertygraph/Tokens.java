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

public class Tokens {

    private final String prefix;
    private final String id;
    private final String label;
    private final String from;
    private final String to;
    private final String fromLabels;
    private final String toLabels;

    public Tokens() {
        this("~");
    }

    public Tokens(String prefix) {
        this.prefix = prefix;
        this.id  = String.format("%sid", prefix);
        this.label  = String.format("%slabel", prefix);
        this.from  = String.format("%sfrom", prefix);
        this.to  = String.format("%sto", prefix);
        this.fromLabels  = String.format("%sfromLabels", prefix);
        this.toLabels  = String.format("%stoLabels", prefix);
    }

    public String prefix() {
        return prefix;
    }

    public String id() {
        return id;
    }

    public String label() {
        return label;
    }

    public String from() {
        return from;
    }

    public String to() {
        return to;
    }

    public String fromLabels() {
        return fromLabels;
    }

    public String toLabels() {
        return toLabels;
    }
}
