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

package software.amazon.neptune;

import com.github.rvesse.airline.annotations.Cli;
import com.github.rvesse.airline.help.Help;

@Cli(name = "java -jar neptune-topology-aware-client-demo.jar",
        description = "Demo of topology aware Gremlin cluster and client",
        defaultCommand = Help.class,
        commands = {
                RefreshAgentDemo.class,
                RollingSubsetOfEndpointDemo.class,
                Help.class})
public class ApplicationRunner {

    public static void main(String[] args) {
        com.github.rvesse.airline.Cli<Runnable> cli = new com.github.rvesse.airline.Cli<>(ApplicationRunner.class);

        String logLevel = "info";

        for (int i = 0; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("--log-level")) {
                logLevel = args[i + 1];
                break;
            }
        }

        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", logLevel);
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.tinkerpop.gremlin.driver.TopologyAwareClient", logLevel);
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.tinkerpop.gremlin.driver", logLevel);
        System.setProperty("org.slf4j.simpleLogger.log.software.amazon.awssdk", "info");
        System.setProperty("org.slf4j.simpleLogger.log.io.netty", "info");

        try {
            Runnable cmd = cli.parse(args);
            cmd.run();
        } catch (Exception e) {

            System.err.println(e.getMessage());

            Runnable cmd = cli.parse("help", args[0]);
            cmd.run();

            System.exit(-1);
        }
    }
}
