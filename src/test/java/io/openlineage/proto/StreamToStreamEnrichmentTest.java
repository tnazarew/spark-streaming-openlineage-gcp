package io.openlineage.proto;

import io.openlineage.SparkKafkaOpenLineageTest;
import io.openlineage.proto.util.TransactionProducers;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class StreamToStreamEnrichmentTest extends SparkKafkaOpenLineageTest {

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
    void testStreamToStreamEnrichment() throws Exception {
        String inputTopic = "raw-transactions";
        String outputTopic = "enriched-transactions";

        // Customers view
        registerCustomersView("src/test/resources/customers.csv");

        // CountDownLatch to coordinate producer and consumer
        CountDownLatch producerFinished = new CountDownLatch(1);

        // Start transaction producer using shared utility - small, fast batches
        Properties producerProps = createProducerProperties();
        Thread producer = TransactionProducers.startBatchingProducer(
                inputTopic, producerProps, producerFinished,
                2, 200, /*keyAsCustomerId*/ true);

        // Read Kafka input via helper
        Dataset<Row> inputStream = readKafkaStream(inputTopic, true);

        inputStream
                .withColumn("kafka_timestamp", functions.col("timestamp"))
                .withColumn("transaction", org.apache.spark.sql.protobuf.functions.from_protobuf(
                        functions.col("value"),
                        "io.openlineage.proto.TransactionEvent",
                        getDescriptorPath("transaction_event.desc")))
                .createOrReplaceTempView("parsed");

        // 2) SQL with CTEs: enrich and build Kafka key/value
        Dataset<Row> kafkaRows = spark.sql(
                "WITH enriched AS (\n" +
                "  SELECT\n" +
                "    p.kafka_timestamp,\n" +
                "    p.transaction.id AS transaction_id,\n" +
                "    p.transaction.transaction_value AS amount,\n" +
                "    p.transaction.customer_id AS customer_id,\n" +
                "    p.transaction.country AS country,\n" +
                "    p.transaction.event_time AS event_time,\n" +
                "    COALESCE(c.customer_name, 'Unknown') AS customer_name,\n" +
                "    COALESCE(c.type, 'unknown') AS customer_type,\n" +
                "    COALESCE(c.nationality, 'Unknown') AS customer_nationality,\n" +
                "    COALESCE(c.credit_limit, 0) AS customer_credit_limit,\n" +
                "    COALESCE(c.registration_date, '1970-01-01') AS customer_registration_date,\n" +
                "    current_timestamp() AS enrichment_timestamp,\n" +
                "    'stream-enrichment' AS processing_type\n" +
                "  FROM parsed p LEFT JOIN customers c ON p.transaction.customer_id = c.customer_id\n" +
                "), final AS (\n" +
                "  SELECT\n" +
                "    CAST(customer_id AS STRING) AS key,\n" +
                "    to_json(named_struct(\n" +
                "      'transaction_id', transaction_id,\n" +
                "      'customer_id', customer_id,\n" +
                "      'amount', amount,\n" +
                "      'country', country,\n" +
                "      'event_time', event_time,\n" +
                "      'customer_name', customer_name,\n" +
                "      'customer_type', customer_type,\n" +
                "      'customer_nationality', customer_nationality,\n" +
                "      'customer_credit_limit', customer_credit_limit,\n" +
                "      'enrichment_timestamp', enrichment_timestamp,\n" +
                "      'processing_type', processing_type\n" +
                "    )) AS value\n" +
                "  FROM enriched\n" +
                ")\n" +
                "SELECT key, value FROM final");

        // Write enriched data to output Kafka topic
        StreamingQuery enrichmentQuery = kafkaRows
                .writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaContainer.getBootstrapServers())
                .option("topic", outputTopic)
                .option("checkpointLocation", "checkpoint/stream-enrichment")
                .outputMode("append")
                .trigger(Trigger.ProcessingTime("200 milliseconds"))
                .start();

        try {
            producerFinished.await(30, TimeUnit.SECONDS);
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        enrichmentQuery.stop();
        producer.interrupt();
    }
}
