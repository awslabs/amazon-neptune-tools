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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

class Headers {

    private final List<Header> headers = new ArrayList<>();

    void add(Header header) {
        headers.add(header);
    }

    Header get(int index){
        return headers.get(index);
    }

    List<String> values(){
        return headers.stream().map(Header::value).collect(Collectors.toList());
    }
}
