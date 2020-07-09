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

package com.amazonaws.services.neptune.cluster;

public enum InstanceType {
    db_r4_large{
        @Override
        int concurrency() {
            return 4;
        }
    },
    db_r4_xlarge{
        @Override
        int concurrency() {
            return 8;
        }
    },
    db_r4_2xlarge{
        @Override
        int concurrency() {
            return 16;
        }
    },
    db_r4_4xlarge{
        @Override
        int concurrency() {
            return 32;
        }
    },
    db_r4_8xlarge{
        @Override
        int concurrency() {
            return 64;
        }
    },
    db_r5_large{
        @Override
        int concurrency() {
            return 4;
        }
    },
    db_r5_xlarge{
        @Override
        int concurrency() {
            return 8;
        }
    },
    db_r5_2xlarge{
        @Override
        int concurrency() {
            return 16;
        }
    },
    db_r5_4xlarge{
        @Override
        int concurrency() {
            return 32;
        }
    },
    db_r5_8xlarge{
        @Override
        int concurrency() {
            return 64;
        }
    },
    db_r5_12xlarge{
        @Override
        int concurrency() {
            return 96;
        }
    },
    db_r5_16xlarge {
        @Override
        int concurrency() {
            return 128;
        }
    },
    db_r5_24xlarge {
        @Override
        int concurrency() {
            return 192;
        }
    },
    db_m5_large {
        @Override
        int concurrency() {
            return 4;
        }
    },
    db_m5_xlarge {
        @Override
        int concurrency() {
            return 8;
        }
    },
    db_m5_2xlarge {
        @Override
        int concurrency() {
            return 16;
        }
    },
    db_m5_3xlarge {
        @Override
        int concurrency() {
            return 32;
        }
    },
    db_m5_8xlarge {
        @Override
        int concurrency() {
            return 64;
        }
    },
    db_m5_12xlarge {
        @Override
        int concurrency() {
            return 96;
        }
    },
    db_m5_16xlarge {
        @Override
        int concurrency() {
            return 128;
        }
    },
    db_m5_24xlarge {
        @Override
        int concurrency() {
            return 192;
        }
    },
    db_t3_medium{
        @Override
        int concurrency() {
            return 4;
        }
    };

    public static InstanceType parse(String value){
        String typeName = value.toLowerCase().replace(".", "_");
        try
        {
            return InstanceType.valueOf(typeName);

        } catch (IllegalArgumentException e){
            return db_r4_2xlarge;
        }
    }

    abstract int concurrency();

    public String value(){
       return name().replace("_", ".");
    }

    @Override
    public String toString() {
        return value();
    }
}
