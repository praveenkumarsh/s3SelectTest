package org.praveen.s3selectdemo.service.impl;

import org.praveen.s3selectdemo.service.GetItemUseCase;
import org.praveen.s3selectdemo.service.S3DataProvider;
import org.springframework.stereotype.Service;

@Service
public class GetItemUseCaseImpl implements GetItemUseCase {

    private final S3DataProvider s3DataProvider;

    public GetItemUseCaseImpl(S3DataProvider s3DataProvider) {
        this.s3DataProvider = s3DataProvider;
    }


    @Override
    public String getItems(String field, String value) {
        return s3DataProvider.returnItemFromBucket(field, value);
    }
}
