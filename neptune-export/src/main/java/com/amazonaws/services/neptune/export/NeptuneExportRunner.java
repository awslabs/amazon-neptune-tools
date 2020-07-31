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

import com.amazonaws.services.neptune.NeptuneExportBaseCommand;
import com.amazonaws.services.neptune.NeptuneExportCli;

public class NeptuneExportRunner {

    private final String[] args;
    private final NeptuneExportEventHandler eventHandler;

    public NeptuneExportRunner(String[] args) {
        this(args, NeptuneExportEventHandler.NULL_EVENT_HANDLER);
    }

    public NeptuneExportRunner(String[] args, NeptuneExportEventHandler eventHandler) {
        this.args = args;
        this.eventHandler = eventHandler;
    }

    public void run(){
        com.github.rvesse.airline.Cli<Runnable> cli = new com.github.rvesse.airline.Cli<>(NeptuneExportCli.class);

        try {
            Runnable cmd = cli.parse(args);

            if (NeptuneExportBaseCommand.class.isAssignableFrom(cmd.getClass())) {
                NeptuneExportBaseCommand baseCommand = (NeptuneExportBaseCommand) cmd;
                baseCommand.applyLogLevel();
                baseCommand.setEventHandler(eventHandler);
            }

            cmd.run();
        } catch (Exception e) {

            System.err.println(e.getMessage());
            System.err.println();

            Runnable cmd = cli.parse("help", args[0]);
            cmd.run();

            System.exit(-1);
        }
    }
}
