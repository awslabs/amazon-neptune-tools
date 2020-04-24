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

package com.amazonaws.services.neptune.propertygraph.io;

import com.amazonaws.services.neptune.io.CommandWriter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JsonResource<T extends Jsonizable> {

    private final String title;
    private final File resourcePath;
    private final Class<? extends Jsonizable> clazz;

    public JsonResource(String title, File resourcePath, Class<? extends Jsonizable> clazz) {
        this.title = title;
        this.resourcePath = resourcePath;
        this.clazz = clazz;
    }

    public void save(Jsonizable object) throws IOException {
        if (resourcePath == null) {
            return;
        }

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(resourcePath), UTF_8))) {
            ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
            String json = objectWriter.writeValueAsString(object.toJson());
            writer.write(json);
        }
    }

    public T get() throws IOException {
        if (resourcePath == null) {
            throw new IllegalStateException("Resource path is null");
        }

        JsonNode json = new ObjectMapper().readTree(resourcePath);

        try {
            Method method = clazz.getMethod("fromJson", JsonNode.class);
            Object o = method.invoke(null, json);
            //noinspection unchecked
            return (T) o;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }


    public void writeResourcePathAsMessage(CommandWriter writer) {
        if (resourcePath == null) {
            return;
        }

        writer.writeMessage(title + " : " + resourcePath.toString());
    }

}
