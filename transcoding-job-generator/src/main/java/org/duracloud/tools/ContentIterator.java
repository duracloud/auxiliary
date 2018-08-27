package org.duracloud.tools;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

/**
 * Provides a way to iterate through all items in an S3 bucket.
 *
 * @author Bill Branan
 * Date: Aug 23, 2018
 */
public class ContentIterator implements Iterator<String> {

    private AmazonS3 s3Client;
    private String bucketName;

    private int index;
    private List<String> contentList;

    public ContentIterator(AmazonS3 s3Client, String bucketName) {
        index = 0;
        this.s3Client = s3Client;
        this.bucketName = bucketName;

        contentList = getCompleteBucketContents(bucketName, null);
    }

    public boolean hasNext() {
        if (index < contentList.size()) {
            return true;
        } else {
            if (contentList.size() > 0) {
                updateList();
                return contentList.size() > 0;
            } else {
                return false;
            }
        }
    }

    public String next() {
        if (hasNext()) {
            String next = contentList.get(index);
            ++index;
            return next;
        } else {
            throw new NoSuchElementException();
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    private void updateList() {
        String lastItem = contentList.get(contentList.size() - 1);
        contentList = getCompleteBucketContents(bucketName, lastItem);
        index = 0;
    }

    private List<String> getCompleteBucketContents(String bucketName, String marker) {
        List<String> contentItems = new ArrayList<>();

        List<S3ObjectSummary> objects = listObjects(bucketName, marker);
        for (S3ObjectSummary object : objects) {
            contentItems.add(object.getKey());
        }
        return contentItems;
    }

    private List<S3ObjectSummary> listObjects(String bucketName, String marker) {
        int numResults = 10000;
        ListObjectsRequest request =
            new ListObjectsRequest(bucketName, null, marker, null, numResults);
        try {
            ObjectListing objectListing = s3Client.listObjects(request);
            return objectListing.getObjectSummaries();
        } catch (AmazonClientException e) {
            String err = "Could not get contents of S3 bucket " + bucketName
                         + " due to error: " + e.getMessage();
            throw new RuntimeException(err);
        }
    }

}