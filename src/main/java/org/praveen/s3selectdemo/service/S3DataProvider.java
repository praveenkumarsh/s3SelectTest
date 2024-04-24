package org.praveen.s3selectdemo.service;

public interface S3DataProvider {

    String returnItemFromBucket(String field, String value);

}
