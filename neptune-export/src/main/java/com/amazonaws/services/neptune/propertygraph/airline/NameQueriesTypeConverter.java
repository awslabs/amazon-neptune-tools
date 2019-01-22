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

package com.amazonaws.services.neptune.propertygraph.airline;

import com.amazonaws.services.neptune.propertygraph.NamedQueries;
import com.github.rvesse.airline.model.ArgumentsMetadata;
import com.github.rvesse.airline.model.OptionMetadata;
import com.github.rvesse.airline.parser.ParseState;
import com.github.rvesse.airline.types.TypeConverter;
import com.github.rvesse.airline.types.TypeConverterProvider;
import com.github.rvesse.airline.types.numerics.NumericTypeConverter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class NameQueriesTypeConverter implements TypeConverter, TypeConverterProvider {
    @Override
    public Object convert(String s, Class<?> aClass, String value) {

        int i = value.indexOf("=");

        if (i < 0){
            throw new IllegalArgumentException("Invalid named query format");
        }

        String name = value.substring(0, i).trim();
        List<String> queries = Arrays.stream(value.substring(i + 1).split(";")).
                map(String::trim).
                collect(Collectors.toList());

        return new NamedQueries(name, queries);
    }

    @Override
    public void setNumericConverter(NumericTypeConverter numericTypeConverter) {
        // Do nothing
    }

    @Override
    public <T> TypeConverter getTypeConverter(OptionMetadata optionMetadata, ParseState<T> parseState) {
        return this;
    }

    @Override
    public <T> TypeConverter getTypeConverter(ArgumentsMetadata argumentsMetadata, ParseState<T> parseState) {
        return this;
    }
}
