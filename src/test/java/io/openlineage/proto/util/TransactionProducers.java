package io.openlineage.proto.util;

import com.google.protobuf.Timestamp;
import io.openlineage.proto.TransactionEventProtos;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class TransactionProducers {

    private TransactionProducers() {}

    public static List<TransactionData> loadTransactionEvents(String path) {
        List<TransactionData> events = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 4) {
                    events.add(new TransactionData(
                            parts[0].trim(), // customer_id
                            Integer.parseInt(parts[1].trim()), // amount
                            parts[2].trim(), // type
                            parts[3].trim()  // timestamp
                    ));
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading transaction events: " + e.getMessage());
        }
        return events;
    }

    public static Thread startBatchingProducer(
            String topic,
            Properties producerProps,
            CountDownLatch finishedLatch,
            int batchSize,
            int batchDelayMillis,
            boolean keyAsCustomerId
    ) {
        List<TransactionData> transactions = loadTransactionEvents("src/test/resources/transaction_events.txt");

        Thread t = new Thread(() -> {
            try {
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);

                for (int i = 0; i < transactions.size(); i += batchSize) {
                    int endIndex = Math.min(i + batchSize, transactions.size());
                    List<TransactionData> batch = transactions.subList(i, endIndex);

                    System.out.println("Sending batch " + (i/batchSize + 1) + " with " + batch.size() + " transactions");

                    for (int j = 0; j < batch.size(); j++) {
                        TransactionData txn = batch.get(j);

                        // Create protobuf message
                        TransactionEventProtos.TransactionEvent protobufEvent =
                                TransactionEventProtos.TransactionEvent.newBuilder()
                                        .setId(i + j)
                                        .setTransactionValue(txn.amount)
                                        .setCustomerId(txn.customerId)
                                        .setCountry("GLOBAL")
                                        .setEventTime(Timestamp.newBuilder()
                                                .setSeconds(Instant.parse(txn.timestamp).getEpochSecond())
                                                .build())
                                        .build();

                        byte[] serializedData = protobufEvent.toByteArray();
                        String key = keyAsCustomerId ? txn.customerId : ("txn-" + txn.customerId + "-" + (i + j));

                        producer.send(new ProducerRecord<>(topic, key, serializedData));
                    }

                    producer.flush();

                    // Wait before sending next batch (except for last batch)
                    if (endIndex < transactions.size()) {
                        Thread.sleep(batchDelayMillis);
                    }
                }

                producer.close();
                System.out.println("Finished sending all transaction batches");

            } catch (Exception e) {
                System.err.println("Error in transaction producer: " + e.getMessage());
                e.printStackTrace();
            } finally {
                finishedLatch.countDown();
            }
        });

        t.start();
        return t;
    }
}

