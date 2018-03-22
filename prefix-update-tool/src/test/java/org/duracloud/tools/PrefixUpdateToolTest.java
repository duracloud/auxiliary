/*
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE and NOTICE files at the root of the source
 * tree and available online at
 *
 *     http://duracloud.org/license/
 */
package org.duracloud.tools;

import java.util.LinkedList;
import java.util.List;

import org.duracloud.client.ContentStore;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Performs tests on the prefix update tool.
 *
 * @author Bill Branan
 * Date: 1/9/2015
 */
public class PrefixUpdateToolTest {

    private ContentStore store;

    private static final String spaceId = "space-id";
    private static final String oldPrefix = "old/prefix/";
    private static final String newPrefix = "new/prefix/";

    private List<String> spaceContents = new LinkedList<>();
    private static final String suffixOne = "uno";
    private static final String suffixTwo = "dos";

    @Before
    public void setup() {
        store = EasyMock.createMock(ContentStore.class);

        spaceContents.add(oldPrefix + suffixOne);
        spaceContents.add(oldPrefix + suffixTwo);
        spaceContents.add("another/prefix/tres");
    }

    public void replayMocks() {
        EasyMock.replay(store);
    }

    @After
    public void teardown() {
        EasyMock.verify(store);
    }

    /**
     * Tests the prefix update, to verify that the old prefix is replaced
     * with the new prefix, and only files which begin with the old prefix
     * are updated.
     *
     * @throws Exception
     */
    @Test
    public void testPrefixUpdate() throws Exception {
        PrefixUpdateTool tool =
            new PrefixUpdateTool(spaceId, "host", "port", "user", "pass",
                                 "store-id", oldPrefix, newPrefix, false);

        EasyMock.expect(store.getSpaceContents(spaceId))
                .andReturn(spaceContents.iterator());

        // The expected calls to update prefix values
        EasyMock.expect(store.moveContent(spaceId,
                                          oldPrefix + suffixOne,
                                          spaceId,
                                          newPrefix + suffixOne)).andReturn("");
        EasyMock.expect(store.moveContent(spaceId,
                                          oldPrefix + suffixTwo,
                                          spaceId,
                                          newPrefix + suffixTwo)).andReturn("");

        replayMocks();

        tool.doUpdate(store, spaceId, oldPrefix, newPrefix);
    }

    /**
     * Tests with the dry-run option turned on. There should be no calls
     * to update content items.
     *
     * @throws Exception
     */
    @Test
    public void testDryRun() throws Exception {
        PrefixUpdateTool tool =
            new PrefixUpdateTool(spaceId, "host", "port", "user", "pass",
                                 "store-id", oldPrefix, newPrefix, true);

        EasyMock.expect(store.getSpaceContents(spaceId))
                .andReturn(spaceContents.iterator());

        replayMocks();

        tool.doUpdate(store, spaceId, oldPrefix, newPrefix);
    }

}
