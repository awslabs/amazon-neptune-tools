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

package software.amazon.utils;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import org.apache.commons.lang3.StringUtils;

public class RegionUtils {
    public static String getCurrentRegionName(){
        String result = EnvironmentVariableUtils.getOptionalEnv("AWS_REGION", null);
        if (StringUtils.isEmpty(result)) {
            Region currentRegion = Regions.getCurrentRegion();
            if (currentRegion != null) {
                result = currentRegion.getName();
            }
        }
        return result;
    }
}
