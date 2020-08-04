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

package software.amazon.neptune.cluster;

import java.util.Map;

public class NeptuneInstanceProperties {

    private final String instanceId;
    private final String endpoint;
    private final String status;
    private final String availabilityZone;
    private final String instanceType;

    private final Map<String, String> tags;

    public NeptuneInstanceProperties(String instanceId,
                                     String endpoint,
                                     String status,
                                     String availabilityZone,
                                     String instanceType,
                                     Map<String, String> tags) {
        this.instanceId = instanceId;
        this.endpoint = endpoint;
        this.status = status;
        this.availabilityZone = availabilityZone;
        this.instanceType = instanceType;
        this.tags = tags;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getStatus() {
        return status;
    }

    public String getAvailabilityZone() {
        return availabilityZone;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public boolean hasTag(String tag){
        return tags.containsKey(tag);
    }

    public String getTag(String tag){
        return tags.get(tag);
    }

    public String getTag(String tag, String defaultValue){
        if (!tags.containsKey(tag)){
           return defaultValue;
        }
        return tags.get(tag);
    }

    public boolean hasTag(String tag, String value){
        return hasTag(tag) && getTag(tag).equals(value);
    }

    public boolean isAvailable(){
        return getStatus().equalsIgnoreCase("Available");
    }

    @Override
    public String toString() {
        return "NeptuneInstanceProperties{" +
                "instanceId='" + instanceId + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", status='" + status + '\'' +
                ", availabilityZone='" + availabilityZone + '\'' +
                ", instanceType='" + instanceType + '\'' +
                ", tags=" + tags +
                '}';
    }
}
