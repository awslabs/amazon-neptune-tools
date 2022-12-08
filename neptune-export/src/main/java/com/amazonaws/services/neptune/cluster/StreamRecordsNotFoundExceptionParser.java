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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamRecordsNotFoundExceptionParser {
    public static class LastEventId {
        private final long commitNum;
        private final long opNum;

        public LastEventId(long commitNum, long opNum) {
            this.commitNum = commitNum;
            this.opNum = opNum;
        }

        public long commitNum() {
            return commitNum;
        }

        public long opNum() {
            return opNum;
        }

        @Override
        public String toString() {
            return "{ " +
                    "\"commitNum\": " + commitNum +
                    ", \"opNum\": " + opNum +
                    " }";
        }
    }

    public static EventId parseLastEventId(String errorMessage){
        String commitNum = "-1";
        String opNum = "-1";

        Pattern p = Pattern.compile("\\d+");
        Matcher m = p.matcher(errorMessage);

        if (m.find()){
            commitNum = m.group();
        }

        if (m.find()){
            opNum = m.group();
        }


        return new EventId(Long.parseLong( commitNum), Long.parseLong(opNum));
    }
}
