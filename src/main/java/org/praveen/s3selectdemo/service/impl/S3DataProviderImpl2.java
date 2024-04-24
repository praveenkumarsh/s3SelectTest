//package org.praveen.s3selectdemo.service.impl;
//
//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.model.*;
//import org.praveen.s3selectdemo.service.S3DataProvider;
//import org.springframework.context.annotation.Configuration;
//
//import java.io.InputStream;
//import java.util.ArrayList;
//import java.util.List;
//
//import static org.praveen.s3selectdemo.model.Constants.BUCKET_NAME;
//
//@Configuration
//public class S3DataProviderImpl2 implements S3DataProvider {
//    private final AmazonS3 s3Client;
//
//    public S3DataProviderImpl2(AmazonS3 s3Client) {
//        this.s3Client = s3Client;
//    }
//
//    @Override
//    public String returnItemFromBucket(String field, String value) {
//        System.out.println("======");
//        long timeS = System.currentTimeMillis();
//        ObjectListing objectListing = s3Client.listObjects(BUCKET_NAME);
//        long timeE = System.currentTimeMillis();
//        System.out.println("======");
//        System.out.println("============ListCall : "+(timeE-timeS));
//
//        List<Object> results = new ArrayList<>();
//
//        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
//            if (!summary.getKey().endsWith("50Line.json")) {
//                continue;
//            }
////            if (!summary.getKey().endsWith(".gz")) {
////                continue;
////            }
////            if (!summary.getKey().endsWith(".json")) {
////                continue;
////            }
//            SelectObjectContentRequest request = new SelectObjectContentRequest();
//            request.setBucketName(BUCKET_NAME);
//            request.setKey(summary.getKey());
////            request.setExpression("SELECT * FROM S3Object s  where s." + field + " = '" + value + "'");
//            request.setExpression("SELECT * FROM S3Object s");
//            request.setExpressionType(ExpressionType.SQL);
////            request.setRequestProgress(new RequestProgress().withEnabled(true));
////            request.setSSECustomerKey(new SSECustomerKey());
//
//            InputSerialization inputSerialization = new InputSerialization();
////            inputSerialization.setJson(new JSONInput().withType(JSONType.DOCUMENT));
//            inputSerialization.setJson(new JSONInput().withType(JSONType.LINES));
////            inputSerialization.setCsv(new CSVInput().withFieldDelimiter(",").withFileHeaderInfo(FileHeaderInfo.USE));
////            inputSerialization.setCompressionType(CompressionType.);
//            OutputSerialization outputSerialization = new OutputSerialization();
//            outputSerialization.withJson(new JSONOutput().withRecordDelimiter(","));
//            //{},{},
//
//            request.setInputSerialization(inputSerialization);
//            request.setOutputSerialization(outputSerialization);
//
//            System.out.println("S3Select job started");
//            long startTimeInMillis = System.currentTimeMillis();
//            long startTimeInMillisForVisit = System.currentTimeMillis();
//
//            try (SelectObjectContentResult result = s3Client.selectObjectContent(request)) {
//
//                // Perform some per-stream output of details of current query
//                try (InputStream resultInputStream = result.getPayload().getRecordsInputStream(new SelectObjectContentEventVisitor() {
//                    @Override
//                    public void visit(SelectObjectContentEvent.StatsEvent event) {
//                        System.out.println("Total events received: " + event.getDetails().getBytesReturned().intValue());
//                    }
//
//                    @Override
//                    public void visit(SelectObjectContentEvent.EndEvent event) {
//                        long endTimeInMillisForVisit = System.currentTimeMillis();
//                        System.out.println("Total Execution Time in millis for Visit : " + (endTimeInMillisForVisit - startTimeInMillisForVisit));
//                        System.out.println("Result is complete");
//                    }
//                })) {
//
//                    long endTimeInMillis = System.currentTimeMillis();
//                    System.out.println("Total Execution Time in millis : " + (endTimeInMillis - startTimeInMillis));
//
////                    return new String(com.amazonaws.util.IOUtils.toByteArray(resultInputStream)).trim();
////                    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
////
////                    int nRead;
////                    byte[] data = new byte[16384];
////
////                    while ((nRead = resultInputStream.read(data, 0, data.length)) != -1) {
////                        buffer.write(data, 0, nRead);
////                    }
////
////                    buffer.flush();
////
////                    results.add(buffer.toString());
//                    results.add(new String(com.amazonaws.util.IOUtils.toByteArray(resultInputStream)).trim());
//                }
//            } catch (java.io.IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        // Concatenate all results into a single JSON array
//        StringBuilder jsonArray = new StringBuilder("[");
//        for (int i = 0; i < results.size(); i++) {
//            String record = (String) results.get(i);
//            if (i == 0) {
//                record = record.substring(0, record.length() - 2) + "}";
//            }
//            jsonArray.append(record);
//        }
//        jsonArray.append("]");
//
//        return jsonArray.toString();
//    }
//
//}
//
