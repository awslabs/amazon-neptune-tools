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

package com.amazonaws.services.neptune.export;

import org.codehaus.plexus.util.cli.CommandLineUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Args {

    private final List<String> args = new ArrayList<>();

    public Args(String cmd) {
        String[] values;
        try {
            values = CommandLineUtils.translateCommandline(cmd);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        args.addAll(Arrays.asList(values));
    }

    public void removeOptions(String... options) {

        for (String option : options) {
            int index = args.indexOf(option);
            while (index >= 0) {
                args.remove(index + 1);
                args.remove(index);
                index = args.indexOf(option);
            }
        }

    }

    public void removeFlags(String... flags) {
        for (String flag : flags) {
            int index = args.indexOf(flag);
            while (index >= 0) {
                args.remove(index);
                index = args.indexOf(flag);
            }
        }
    }

    public void addOption(String option, String value) {
        args.add(option);
        args.add(value);
    }

    public boolean contains(String name){
        for (String arg : args) {
            if (arg.equals(name)){
                return true;
            }
        }
        return false;
    }

    public String[] values() {
        return args.toArray(new String[]{});
    }

    @Override
    public String toString() {
        return String.join(" ", args);
    }
}
