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

package com.amazonaws.services.neptune.profiles.incremental_export;

import com.amazonaws.services.neptune.cluster.EventId;
import com.amazonaws.services.neptune.cluster.StreamRecordsNotFoundExceptionParser;
import org.junit.Test;

import static org.junit.Assert.*;

public class StreamRecordsNotFoundExceptionParserTest {

    @Test
    public void ShouldParseCommitNumAndOpNum(){
        String errorMessage = "Requested startEventId is from the future. Last valid eventId is [commitNum = 1132, opNum = 200]";

        EventId lastEventId = StreamRecordsNotFoundExceptionParser.parseLastEventId(errorMessage);

        assertEquals(1132, lastEventId.commitNum());
        assertEquals(200, lastEventId.opNum());
    }

    @Test
    public void ShouldReturnMinus1IfNotFound(){
        String errorMessage = "Requested startEventId is from the future";

        EventId lastEventId = StreamRecordsNotFoundExceptionParser.parseLastEventId(errorMessage);

        assertEquals(-1, lastEventId.commitNum());
        assertEquals(-1, lastEventId.opNum());
    }

}