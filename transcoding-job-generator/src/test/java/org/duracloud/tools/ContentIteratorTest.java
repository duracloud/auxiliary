package org.duracloud.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Test;

/**
 * @author Bill Branan
 * Date: Aug 24, 2018
 */
public class ContentIteratorTest extends JobGeneratorTestBase {

    /**
     * Verifies that the content iterator is able to iterate through
     * content in S3 even when multiple listObjects() calls are needed
     * to retrieve the entire list.
     */
    @Test
    public void testContentIterator() {
        AmazonS3 s3Client = EasyMock.createMock(AmazonS3.class);
        String bucketName = "bucket-name";

        // Set 1
        List<S3ObjectSummary> objectSummaries1 = new ArrayList<>();
        String itemId1 = "";
        for (int i = 1; i <= 1000; i++) {
            itemId1 = "item-" + i;
            S3ObjectSummary objSummmary = new S3ObjectSummary();
            objSummmary.setBucketName(bucketName);
            objSummmary.setKey(itemId1);
            objectSummaries1.add(objSummmary);
        }
        SettableObjectListing objectListing1 = new SettableObjectListing();
        objectListing1.setObjectSummaries(objectSummaries1);
        objectListing1.setTruncated(true);

        Capture<ListObjectsRequest> listRequest1Capture = Capture.newInstance();
        EasyMock.expect(s3Client.listObjects(EasyMock.capture(listRequest1Capture)))
                .andReturn(objectListing1);

        // Set 2
        List<S3ObjectSummary> objectSummaries2 = new ArrayList<>();
        String itemId2 = "";
        for (int i = 1001; i <= 1321; i++) {
            itemId2 = "item-" + i;
            S3ObjectSummary objSummmary = new S3ObjectSummary();
            objSummmary.setBucketName(bucketName);
            objSummmary.setKey(itemId2);
            objectSummaries2.add(objSummmary);
        }
        SettableObjectListing objectListing2 = new SettableObjectListing();
        objectListing2.setObjectSummaries(objectSummaries2);
        objectListing2.setTruncated(false);

        Capture<ListObjectsRequest> listRequest2Capture = Capture.newInstance();
        EasyMock.expect(s3Client.listObjects(EasyMock.capture(listRequest2Capture)))
                .andReturn(objectListing2);

        // Set 3
        List<S3ObjectSummary> objectSummaries3 = new ArrayList<>();
        SettableObjectListing objectListing3 = new SettableObjectListing();
        objectListing3.setObjectSummaries(objectSummaries3);
        objectListing3.setTruncated(false);

        Capture<ListObjectsRequest> listRequest3Capture = Capture.newInstance();
        EasyMock.expect(s3Client.listObjects(EasyMock.capture(listRequest3Capture)))
                .andReturn(objectListing3);

        EasyMock.replay(s3Client);

        ContentIterator contentIterator = new ContentIterator(s3Client, bucketName);
        int counter = 0;
        while (contentIterator.hasNext()) {
            counter++;
            String contentId = contentIterator.next();
            assertEquals("item-" + counter, contentId);
        }

        assertEquals(1321, counter);

        EasyMock.verify(s3Client);

        ListObjectsRequest listRequest1 = listRequest1Capture.getValue();
        assertEquals(bucketName, listRequest1.getBucketName());
        assertNull(listRequest1.getMarker());

        ListObjectsRequest listRequest2 = listRequest2Capture.getValue();
        assertEquals(bucketName, listRequest2.getBucketName());
        assertEquals(itemId1, listRequest2.getMarker());

        ListObjectsRequest listRequest3 = listRequest3Capture.getValue();
        assertEquals(bucketName, listRequest3.getBucketName());
        assertEquals(itemId2, listRequest3.getMarker());
    }

}
