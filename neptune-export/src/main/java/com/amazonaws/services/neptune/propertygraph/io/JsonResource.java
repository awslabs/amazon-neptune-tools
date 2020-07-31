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
import com.amazonaws.services.neptune.util.S3ObjectInfo;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;

import static java.nio.charset.StandardCharsets.UTF_8;

public class JsonResource<T extends Jsonizable> {

    private final String title;
    private final URI resourcePath;
    private final Class<? extends Jsonizable> clazz;

    public JsonResource(String title, URI resourcePath, Class<? extends Jsonizable> clazz) {
        this.title = title;
        this.resourcePath = resourcePath;
        this.clazz = clazz;
    }

    public void save(Jsonizable object) throws IOException {

        if (resourcePath == null) {
            return;
        }

        if (resourcePath.getScheme() != null &&
                (resourcePath.getScheme().equals("s3") || resourcePath.getScheme().equals("https"))) {
            return;
        }

        File resourceFile = new File(resourcePath);

        try (Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(resourceFile), UTF_8))) {
            ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
            String json = objectWriter.writeValueAsString(object.toJson());
            writer.write(json);
        }
    }

    public T get() throws IOException {
        if (resourcePath == null) {
            throw new IllegalStateException("Resource path is null");
        }

        JsonNode json = readJson();

        try {
            Method method = clazz.getMethod("fromJson", JsonNode.class);
            Object o = method.invoke(null, json);
            //noinspection unchecked
            return (T) o;
        } catch (NoSuchMethodException e){
            throw new RuntimeException("Jsonizable object must have a static fromJson(JsonNode) method");
        } catch  ( IllegalAccessException | InvocationTargetException e){
            throw new RuntimeException(e);
        }
    }

    public void writeResourcePathAsMessage(CommandWriter writer) {
        if (resourcePath == null) {
            return;
        }

        writer.writeMessage(title + " : " + resourcePath.toString());
    }

    private JsonNode readJson() throws IOException {
        switch (resourcePath.getScheme()) {
            case "https":
                return new ObjectMapper().readTree(resourcePath.toURL());
            case "s3":
                S3ObjectInfo s3ObjectInfo = new S3ObjectInfo(resourcePath.toString());
                AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
                try (InputStream stream  = s3.getObject(s3ObjectInfo.bucket(), s3ObjectInfo.key()).getObjectContent()){
                    return new ObjectMapper().readTree(stream);
                }
            default:
                File resourceFile = new File(resourcePath.toString());
                if (!resourceFile.exists()) {
                    throw new IllegalStateException(String.format("%s does not exist", resourceFile));
                }
                if (resourceFile.isDirectory()) {
                    throw new IllegalStateException(String.format("Expected a file, but found a directory: %s", resourceFile));
                }
                return new ObjectMapper().readTree(resourceFile);
        }

    }

}
