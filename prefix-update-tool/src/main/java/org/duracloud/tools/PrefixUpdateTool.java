package org.duracloud.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.duracloud.client.ContentStore;
import org.duracloud.client.ContentStoreManager;
import org.duracloud.client.ContentStoreManagerImpl;
import org.duracloud.common.model.Credential;
import org.duracloud.common.util.DateUtil;
import org.duracloud.error.ContentStoreException;

/*
 * Prefix Update Tool - Provides a simple way to update the prefix value on a
 *                      set of content items in a DuraCloud space.
 *
 * @author: Bill Branan
 * Date: Jan 6, 2015
 */
public class PrefixUpdateTool {

    private static final String DEFAULT_PORT = "443";
    private static final String DEFAULT_CONTEXT = "durastore";

    private String spaceName;
    private String host;
    private String port;
    private String username;
    private String password;
    private String storeId;
    private String oldPrefix;
    private String newPrefix;
    private boolean dryRun;

    private static Options cmdOptions;

    public PrefixUpdateTool(String spaceName,
                            String host,
                            String port,
                            String username,
                            String password,
                            String storeId,
                            String oldPrefix,
                            String newPrefix,
                            boolean dryRun) {
        this.spaceName = spaceName;
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.storeId = storeId;
        this.oldPrefix = oldPrefix;
        this.newPrefix = newPrefix;
        this.dryRun = dryRun;
    }

    /**
     * Kicks off the execution of the tool.
     *
     * @throws ContentStoreException
     * @throws IOException
     */
    public void run() throws ContentStoreException, IOException {
        System.out.println("-----------------------------------------" +
                           "\nRunning Prefix Update Tool with config:" +
                           "\nspace name=" + spaceName +
                           "\nhost=" + host +
                           "\nport=" + port +
                           "\nprefix to replace=" + oldPrefix +
                           "\nnew prefix=" + newPrefix +
                           (dryRun ? "\nThis execution is a DRY RUN - no changes will be made!" : "") +
                           "\n-----------------------------------------");

        System.out.println("Setting up tool...");
        ContentStoreManager storeManager =
            new ContentStoreManagerImpl(host, port, DEFAULT_CONTEXT);
        Credential credential = new Credential(username, password);
        storeManager.login(credential);

        ContentStore store;
        if (storeId == null || storeId.equals("")) {
            store = storeManager.getPrimaryContentStore();
            this.storeId = store.getStoreId();
        } else {
            store = storeManager.getContentStore(storeId);
        }

        doUpdate(store, spaceName, oldPrefix, newPrefix);

        System.out.println("Prefix Update Tool process complete.");
    }

    /**
     * Performs the prefix updates. Any content items which begin with the
     * old prefix value are changed to remove the old prefix and replace it
     * with the new prefix.
     *
     * @param store     - DuraCloud storage client
     * @param spaceId   - the space in which to update content items
     * @param oldPrefix - the prefix to replace
     * @param newPrefix - the prefix to add
     * @throws ContentStoreException
     */
    protected void doUpdate(ContentStore store,
                            String spaceId,
                            String oldPrefix,
                            String newPrefix)
        throws ContentStoreException {
        Iterator<String> contentIterator = store.getSpaceContents(spaceId);
        File contentListing =
            new File("original-content-listing-" + DateUtil.nowPlain());
        contentListing.deleteOnExit();

        System.out.println("Retrieving Content Item List...");
        try (BufferedWriter writer =
                 Files.newBufferedWriter(contentListing.toPath(),
                                         StandardCharsets.UTF_8)) {
            while (contentIterator.hasNext()) {
                String contentId = contentIterator.next();
                writer.write(contentId);
                writer.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException("Unable to write content item listing " +
                                       "due to error: " + e.getMessage());
        }

        System.out.println("Beginning Updates...");
        try (BufferedReader reader =
                 Files.newBufferedReader(contentListing.toPath(),
                                         StandardCharsets.UTF_8)) {
            String contentId;
            while ((contentId = reader.readLine()) != null) {
                if (contentId.startsWith(oldPrefix)) {
                    String newContentId =
                        newPrefix + contentId.substring(oldPrefix.length());
                    System.out.println("Updating " + contentId +
                                       " to " + newContentId);
                    if (!dryRun) {
                        store.moveContent(spaceId, contentId, spaceId, newContentId);
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading content item listing: " +
                                       e.getMessage());
        }
    }

    /**
     * Manages the command line execution, including all command line parameters
     *
     * @param args - command line arguments
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        cmdOptions = new Options();

        Option spaceNameOption =
            new Option("s", "spacename", true,
                       "the name of the space in which content will be updated");
        spaceNameOption.setRequired(true);
        cmdOptions.addOption(spaceNameOption);

        Option hostOption =
            new Option("h", "host", true,
                       "the host address of the DuraCloud DuraStore application");
        hostOption.setRequired(true);
        cmdOptions.addOption(hostOption);

        Option portOption =
            new Option("t", "port", true,
                       "the port of the DuraCloud DuraStore application " +
                       "(optional, default value is " + DEFAULT_PORT + ")");
        portOption.setRequired(false);
        cmdOptions.addOption(portOption);

        Option usernameOption =
            new Option("u", "username", true,
                       "the username necessary to perform writes to DuraStore");
        usernameOption.setRequired(true);
        cmdOptions.addOption(usernameOption);

        Option passwordOption =
            new Option("p", "password", true,
                       "the password necessary to perform writes to DuraStore");
        passwordOption.setRequired(true);
        cmdOptions.addOption(passwordOption);

        Option storeIdOption =
            new Option("i", "store-id", true,
                       "the ID of the store (optional)");
        storeIdOption.setRequired(false);
        cmdOptions.addOption(storeIdOption);

        Option oldPrefixOption =
            new Option("o", "old-prefix", true,
                       "the original prefix that should be replaced - " +
                       "only files with this prefix will be updated");
        oldPrefixOption.setRequired(true);
        cmdOptions.addOption(oldPrefixOption);

        Option newPrefixOption =
            new Option("n", "new-prefix", true,
                       "the new prefix to apply");
        newPrefixOption.setRequired(true);
        cmdOptions.addOption(newPrefixOption);

        Option dryRunOption =
            new Option("d", "dry-run", false,
                       "designate this execution as a dry run, no changes " +
                       "will be made, but output will indicate what would have happened");
        dryRunOption.setRequired(false);
        cmdOptions.addOption(dryRunOption);

        CommandLine cmd = null;
        try {
            CommandLineParser parser = new PosixParser();
            cmd = parser.parse(cmdOptions, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            usage();
        }

        String spaceName = cmd.getOptionValue("s");
        String host = cmd.getOptionValue("h");
        String username = cmd.getOptionValue("u");
        String password = cmd.getOptionValue("p");
        String storeId = cmd.getOptionValue("i");
        String oldPrefix = cmd.getOptionValue("o");
        String newPrefix = cmd.getOptionValue("n");

        String port = cmd.getOptionValue("t");
        if (port == null || port.equals("")) {
            port = DEFAULT_PORT;
        }

        boolean dryRun = false;
        if (cmd.hasOption("d")) {
            dryRun = true;
        }

        if (oldPrefix.equals(newPrefix)) {
            System.out.println("The old and new prefix values cannot match!");
            usage();
        }

        PrefixUpdateTool tool =
            new PrefixUpdateTool(spaceName, host, port, username,
                                 password, storeId, oldPrefix, newPrefix,
                                 dryRun);
        tool.run();
    }

    /**
     * Called when the command line arguments are not valid. Prints information
     * about how the tool should be used and exits.
     */
    private static void usage() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Running the Prefix Update Tool", cmdOptions);
        System.exit(1);
    }

}