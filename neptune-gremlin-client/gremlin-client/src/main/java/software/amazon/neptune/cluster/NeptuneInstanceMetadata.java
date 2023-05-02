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

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class NeptuneInstanceMetadata {

    private static final Collection<String> AVAILABLE_STATES = Arrays.asList("available", "backing-up", "modifying", "upgrading");

    private String instanceId;
    private String role;
    private String endpoint;
    private String status;
    private String availabilityZone;
    private String instanceType;

    private final Map<String, String> tags = new HashMap<>();

    public NeptuneInstanceMetadata() {

    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public void setAvailabilityZone(String availabilityZone) {
        this.availabilityZone = availabilityZone;
    }

    public void setInstanceType(String instanceType) {
        this.instanceType = instanceType;
    }

    public void setTags(Map<String, String> tags) {
        this.tags.clear();
        this.tags.putAll(tags);
    }

    public NeptuneInstanceMetadata withInstanceId(String instanceId) {
        setInstanceId(instanceId);
        return this;
    }

    public NeptuneInstanceMetadata withRole(String role) {
        setRole(role);
        return this;
    }

    public NeptuneInstanceMetadata withEndpoint(String endpoint) {
        setEndpoint(endpoint);
        return this;
    }

    public NeptuneInstanceMetadata withStatus(String status) {
        setStatus(status);
        return this;
    }

    public NeptuneInstanceMetadata withAvailabilityZone(String availabilityZone) {
        setAvailabilityZone(availabilityZone);
        return this;
    }

    public NeptuneInstanceMetadata withInstanceType(String instanceType) {
        setInstanceType(instanceType);
        return this;
    }

    public NeptuneInstanceMetadata withTags(Map<String, String> tags) {
        setTags(tags);
        return this;
    }

    public String getInstanceId() {
        return instanceId;
    }

    public String getRole() {
        return role;
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

    public Map<String, String> getTags() {
        return tags;
    }

    public boolean hasTag(String tag) {
        return tags.containsKey(tag);
    }

    public String getTag(String tag) {
        return tags.get(tag);
    }

    public String getTag(String tag, String defaultValue) {
        if (!tags.containsKey(tag)) {
            return defaultValue;
        }
        return tags.get(tag);
    }

    public boolean hasTag(String tag, String value) {
        return hasTag(tag) && getTag(tag).equals(value);
    }

    @JsonIgnore
    public boolean isAvailable() {
        return endpoint != null && AVAILABLE_STATES.contains(getStatus().toLowerCase());
    }

    @JsonIgnore
    public boolean isPrimary() {
        return getRole().equalsIgnoreCase("writer");
    }

    @JsonIgnore
    public boolean isReader() {
        return getRole().equalsIgnoreCase("reader");
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
