/*
 * Copyright 2012-2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client.rebalance;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.log4j.Logger;

import voldemort.VoldemortException;
import voldemort.cluster.Cluster;
import voldemort.store.StoreDefinition;
import voldemort.utils.CmdUtils;
import voldemort.utils.Entropy;
import voldemort.xml.ClusterMapper;
import voldemort.xml.StoreDefinitionsMapper;

import com.google.common.base.Joiner;

// TODO: Drop this tool once SREs are comfortable with KeySampler and
// KeyVersionFetcher tool chain.
@Deprecated
public class RebalanceCLI {

    private final static int SUCCESS_EXIT_CODE = 0;
    private final static int ERROR_EXIT_CODE = 1;
    private final static int HELP_EXIT_CODE = 2;
    private final static Logger logger = Logger.getLogger(RebalanceCLI.class);

    public static void main(String[] args) throws Exception {
        int exitCode = ERROR_EXIT_CODE;
        try {
            OptionParser parser = new OptionParser();
            parser.accepts("help", "Print usage information");
            parser.accepts("current-cluster", "Path to current cluster xml")
                  .withRequiredArg()
                  .describedAs("cluster.xml");
            parser.accepts("current-stores", "Path to store definition xml")
                  .withRequiredArg()
                  .describedAs("stores.xml");
            parser.accepts("entropy",
                           "True - if we want to run the entropy calculator. False - if we want to store keys")
                  .withRequiredArg()
                  .ofType(Boolean.class);
            parser.accepts("output-dir",
                           "Specify the output directory for (1) dumping metadata"
                                   + "(b) dumping entropy keys")
                  .withRequiredArg()
                  .ofType(String.class)
                  .describedAs("path");
            parser.accepts("keys",
                           "The number of keys to use for entropy calculation [ Default : "
                                   + Entropy.DEFAULT_NUM_KEYS + " ]")
                  .withRequiredArg()
                  .ofType(Long.class)
                  .describedAs("num-keys");
            parser.accepts("verbose-logging",
                           "Verbose logging such as keys found missing on specific nodes during post-rebalancing entropy verification");

            OptionSet options = parser.parse(args);

            if(options.has("help")) {
                printHelp(System.out, parser);
                System.exit(HELP_EXIT_CODE);
            }

            // Entropy tool

            Set<String> missing = CmdUtils.missing(options,
                                                   "entropy",
                                                   "output-dir",
                                                   "current-cluster",
                                                   "current-stores");
            if(missing.size() > 0) {
                System.err.println("Missing required arguments: " + Joiner.on(", ").join(missing));
                printHelp(System.err, parser);
                System.exit(ERROR_EXIT_CODE);
            }

            String currentClusterXML = (String) options.valueOf("current-cluster");
            String currentStoresXML = (String) options.valueOf("current-stores");

            Cluster currentCluster = new ClusterMapper().readCluster(new File(currentClusterXML));
            List<StoreDefinition> storeDefs = new StoreDefinitionsMapper().readStoreList(new File(currentStoresXML));
            String outputDir = (String) options.valueOf("output-dir");

            boolean entropy = (Boolean) options.valueOf("entropy");
            boolean verbose = options.has("verbose-logging");
            long numKeys = CmdUtils.valueOf(options, "keys", Entropy.DEFAULT_NUM_KEYS);
            Entropy generator = new Entropy(-1, numKeys, verbose);
            generator.generateEntropy(currentCluster, storeDefs, new File(outputDir), entropy);

            if(logger.isInfoEnabled()) {
                logger.info("Successfully completed entropy check.");
            }
            exitCode = SUCCESS_EXIT_CODE;

        } catch(VoldemortException e) {
            logger.error("Entropy check unsuccessfull- " + e.getMessage(), e);
        } catch(Throwable e) {
            logger.error(e.getMessage(), e);
        }
        System.exit(exitCode);
    }

    public static void printHelp(PrintStream stream, OptionParser parser) throws IOException {
        stream.println("Commands supported");
        stream.println("------------------");
        stream.println();
        stream.println("ENTROPY");
        stream.println("a) --current-cluster <path> --current-stores <path> --entropy <true / false> --output-dir <path> [ Runs the entropy calculator if "
                       + "--entropy is true. Else dumps keys to the directory ]");
        stream.println("\t (1) --keys [ Number of keys ( per store ) we calculate entropy for ]");
        stream.println("\t (2) --verbose-logging [ print keys found missing during entropy ]");
        parser.printHelpOn(stream);
    }
}
