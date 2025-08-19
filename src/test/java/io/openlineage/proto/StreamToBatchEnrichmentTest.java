package io.openlineage.proto;

import io.openlineage.SparkKafkaOpenLineageTest;
import io.openlineage.proto.util.TransactionProducers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StreamToBatchEnrichmentTest extends SparkKafkaOpenLineageTest {

    protected String getTestName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected String getKeySerializer() {
        return StringSerializer.class.getName();
    }

    @Override
    protected String getValueSerializer() {
        return ByteArraySerializer.class.getName();
    }

    @Test
    void testTransactionStreamProcessing() throws Exception {
        String rawEventTopic = "raw-event-topic";

        // Customers view
        registerCustomersView("src/test/resources/customers.csv");

        // Create Derby table for transaction aggregates
        String aggregateTableName = "transaction_aggregates";
        spark.sql("DROP TABLE IF EXISTS " + aggregateTableName);
        spark.sql(String.format(
                "CREATE TABLE %s (" +
                        "window_start TIMESTAMP, " +
                        "window_end TIMESTAMP, " +
                        "customer_id STRING, " +
                        "customer_name STRING, " +
                        "customer_type STRING, " +
                        "nationality STRING, " +
                        "transaction_count LONG, " +
                        "total_amount DECIMAL(15,2), " +
                        "avg_amount DECIMAL(15,2)) " +
                        "USING parquet " +
                        "LOCATION 'spark-warehouse/%s'",
                aggregateTableName, aggregateTableName));

        // CountDownLatch to coordinate producer and consumer
        CountDownLatch producerFinished = new CountDownLatch(1);

        // Start transaction producer using shared utility
        Properties producerProps = createProducerProperties();
        Thread producer = TransactionProducers.startBatchingProducer(
                rawEventTopic, producerProps, producerFinished,
                200, 3000, /*keyAsCustomerId*/ false);

        // Read Kafka stream via helper
        Dataset<Row> rawStream = readKafkaStream(rawEventTopic, false);

        rawStream
                .withColumn("transaction", org.apache.spark.sql.protobuf.functions.from_protobuf(
                        functions.col("value"),
                        "io.openlineage.proto.TransactionEvent",
                        getDescriptorPath("transaction_event.desc")))
                .withWatermark("timestamp", "1 second")
                .createOrReplaceTempView("parsed");

        // Single SQL with WITH (CTE): flatten minimal fields, aggregate, and join to customers
        Dataset<Row> windowedAggregates = spark.sql(
                "WITH txn_agg AS (\n" +
                "  SELECT\n" +
                "    window(timestamp, '10 seconds', '10 seconds', '0 second') AS window,\n" +
                "    transaction.customer_id as customer_id,\n" +
                "    COUNT(*) AS transaction_count,\n" +
                "    SUM(transaction.transaction_value) AS total_amount,\n" +
                "    AVG(transaction.transaction_value) AS avg_amount\n" +
                "  FROM parsed\n" +
                "  GROUP BY window(timestamp, '10 seconds', '10 seconds', '0 second'), transaction.customer_id\n" +
                ")\n" +
                "SELECT\n" +
                "  a.window.start AS window_start,\n" +
                "  a.window.end AS window_end,\n" +
                "  a.customer_id AS customer_id,\n" +
                "  COALESCE(c.customer_name, 'Unknown') AS customer_name,\n" +
                "  COALESCE(c.type, 'unknown') AS customer_type,\n" +
                "  COALESCE(c.nationality, 'Unknown') AS nationality,\n" +
                "  a.transaction_count,\n" +
                "  CAST(a.total_amount AS DECIMAL(15,2)) AS total_amount,\n" +
                "  CAST(a.avg_amount AS DECIMAL(15,2)) AS avg_amount\n" +
                "FROM txn_agg a\n" +
                "LEFT JOIN customers c ON a.customer_id = c.customer_id");

        // Write aggregated results to Derby table using append mode (required for file sinks)
        StreamingQuery query = windowedAggregates
                .writeStream()
                .outputMode("append")
                .option("checkpointLocation", "checkpoint/" + aggregateTableName)
                .toTable(aggregateTableName);

        try {
            // Wait for producer to finish and some processing time
            producerFinished.await(30, TimeUnit.SECONDS);
            Thread.sleep(10000); // Allow time for processing

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        query.stop();
        producer.interrupt();
    }
}
