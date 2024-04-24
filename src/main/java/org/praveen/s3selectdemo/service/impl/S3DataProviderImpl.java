package org.praveen.s3selectdemo.service.impl;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import org.praveen.s3selectdemo.service.S3DataProvider;
import org.springframework.context.annotation.Configuration;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.praveen.s3selectdemo.model.Constants.BUCKET_NAME;

@Configuration
public class S3DataProviderImpl implements S3DataProvider {
    private final AmazonS3 s3Client;
    List<Object> results = new ArrayList<>();

    public S3DataProviderImpl(AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }

    @Override
    public String returnItemFromBucket(String field, String value) {
        System.out.println("======");
        long timeS = System.currentTimeMillis();
        ObjectListing objectListing = s3Client.listObjects(BUCKET_NAME);
        long timeE = System.currentTimeMillis();
        System.out.println("======");
        System.out.println("============ListCall : " + (timeE - timeS));


        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            if (!summary.getKey().endsWith(".csv")) {
                continue;
            }
            SelectObjectContentRequest request = new SelectObjectContentRequest();
            request.setBucketName(BUCKET_NAME);
            request.setKey(summary.getKey());
//            request.setExpression("SELECT * FROM S3Object s  where s." + field + " = '" + value + "'");
            request.setExpression("SELECT * FROM S3Object s");
            request.setExpressionType(ExpressionType.SQL);
            InputSerialization inputSerialization = new InputSerialization();
            inputSerialization.setCsv(new CSVInput().withFieldDelimiter(",").withFileHeaderInfo(FileHeaderInfo.USE));
//            inputSerialization.setJson(new JSONInput().withType(JSONType.LINES));
            OutputSerialization outputSerialization = new OutputSerialization();
            outputSerialization.withJson(new JSONOutput().withRecordDelimiter(","));
            request.setInputSerialization(inputSerialization);
            request.setOutputSerialization(outputSerialization);

            ////////////////////

            ExecutorService es = Executors.newFixedThreadPool(105);
            List<Runnable> tasks = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                Runnable task = new MyTask("Thread " + i, request);
                tasks.add(task);
            }
            CompletableFuture<?>[] futures = tasks.stream()
                    .map(task -> CompletableFuture.runAsync(task, es))
                    .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(futures).join();
            es.shutdown();

//            /////////////
//            // Create an ExecutorService with a fixed thread pool of size 3
//            ExecutorService executor = Executors.newFixedThreadPool(3);
//            // Submit tasks to the ExecutorService
//            for (int i = 1; i <= 1; i++) {
//                Runnable task = new MyTask("Thread " + i, request);
//                executor.submit(task);
//            }
            // Shutdown the ExecutorService when all tasks are completed
            // Shutdown the ExecutorService when all tasks are completed
//            executor.shutdown();
//            try {
//                // Wait until all tasks have completed execution after a shutdown request
//                executor.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.NANOSECONDS);
//            } catch (InterruptedException e) {
//                // Handle interrupted exception
//                e.printStackTrace();
//            }
        }

        // Concatenate all results into a single JSON array
        StringBuilder jsonArray = new StringBuilder("[");
        for (int i = 0; i < results.size(); i++) {
            String record = (String) results.get(i);
            if (i == 0) {
                record = record.substring(0, record.length() - 2) + "}";
            }
            jsonArray.append(record);
        }
        jsonArray.append("]");

        return jsonArray.toString();
    }

    private void s3SelectCall(SelectObjectContentRequest request) {

        System.out.println("S3Select job started");
        long startTimeInMillis = System.currentTimeMillis();
        long startTimeInMillisForVisit = System.currentTimeMillis();
        try (SelectObjectContentResult result = s3Client.selectObjectContent(request)) {
            // Perform some per-stream output of details of current query
            try (InputStream resultInputStream = result.getPayload().getRecordsInputStream(new SelectObjectContentEventVisitor() {
                @Override
                public void visit(SelectObjectContentEvent.StatsEvent event) {
                    System.out.println("Total events received: " + event.getDetails().getBytesReturned().intValue());
                }

                @Override
                public void visit(SelectObjectContentEvent.EndEvent event) {
                    long endTimeInMillisForVisit = System.currentTimeMillis();
                    System.out.println("Total Execution Time in millis for Visit : " + (endTimeInMillisForVisit - startTimeInMillisForVisit));
                    System.out.println("Result is complete");
//                    String res = "res ";
//                    results.add(res);
                }
            })) {

                long endTimeInMillis = System.currentTimeMillis();
                System.out.println("Total Execution Time in millis : " + (endTimeInMillis - startTimeInMillis));
                String res = new String(com.amazonaws.util.IOUtils.toByteArray(resultInputStream)).trim();
//                String res = "res ";
                results.add(res);
            }
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
//        return  "";
    }

    class MyTask implements Runnable {
        private final String threadName;
        private final SelectObjectContentRequest request;

        public MyTask(String threadName, SelectObjectContentRequest request) {
            this.threadName = threadName;
            this.request = request;
        }

        @Override
        public void run() {
            System.out.println("Thread " + threadName + " is running");
            // Simulate some task
            s3SelectCall(request);
//            results.add(res);
            System.out.println("Thread " + threadName + " finished.");
        }
    }

}

