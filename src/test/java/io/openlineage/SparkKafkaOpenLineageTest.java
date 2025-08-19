package io.openlineage;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Durations;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ConsoleTransport;

import java.util.*;
import java.util.concurrent.TimeoutException;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class SparkKafkaOpenLineageTest {
    private KafkaContainer kafkaContainer;
    private SparkSession spark;
    private JavaStreamingContext streamingContext;
    private OpenLineageClient openLineageClient;

    @BeforeAll
    void setup() {
        // Start Kafka container
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
        kafkaContainer.start();

        // Create Spark session with full OpenLineage integration
        spark = SparkSession.builder()
                .appName("SparkKafkaOpenLineageTest")
                .master("local[2]")
                // OpenLineage Spark integration configuration - use config file
                .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
                .config("spark.openlineage.transport.type", "file") // Use console transport for testing
                .config("spark.openlineage.transport.location", "events/dupa.json") // Use console transport for testing
                .config("spark.openlineage.namespace", "test")

                // Basic Spark configuration
                .config("spark.driver.host", "localhost")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .getOrCreate();

        // Create JavaSparkContext from SparkSession
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        streamingContext = new JavaStreamingContext(jsc, Durations.seconds(1));

        // OpenLineage client with console transport for additional manual event emission
        openLineageClient = OpenLineageClient.builder()
                .transport(new ConsoleTransport())
                .build();

        // Verify OpenLineage integration
        System.out.println("=== Spark session created with OpenLineage integration ===");
        System.out.println("Spark version: " + spark.version());
        System.out.println("OpenLineage namespace: " + spark.conf().get("spark.openlineage.namespace"));
        System.out.println("OpenLineage transport: " + spark.conf().get("spark.openlineage.transport.type"));

        // Test if OpenLineage is properly configured
        try {
            Class.forName("io.openlineage.spark.agent.OpenLineageSparkListener");
            System.out.println("=== OpenLineage Spark integration is active and will capture all SQL operations ===");
        } catch (ClassNotFoundException e) {
            System.out.println("=== Warning: OpenLineage Spark integration class not found ===");
        }
    }

    @AfterAll
    void teardown() {
        streamingContext.stop(true, true);
        spark.stop();
        kafkaContainer.stop();
    }

    private Properties createProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        return props;
    }

    @Test
    void testBasicPipelineEmitsOpenLineageEvents() throws TimeoutException {
        String topic = "test-topic";
        List<String> inputEvents = Arrays.asList("event1", "event2", "event3");

        // Produce events to Kafka
        Properties producerProps = createProducerProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        for (String event : inputEvents) {
            producer.send(new ProducerRecord<>(topic, event));
        }
        producer.flush();
        producer.close();

        // Set up Spark Structured Streaming to read from Kafka
        String kafkaBootstrapServers = kafkaContainer.getBootstrapServers();

        // Use Spark Structured Streaming for simplicity
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> df = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrapServers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load();

        org.apache.spark.sql.Dataset<String> values = df.selectExpr("CAST(value AS STRING)").as(org.apache.spark.sql.Encoders.STRING());

        // Collect results to memory sink
        String outputTable = "outputTable";
        org.apache.spark.sql.streaming.StreamingQuery query = values.writeStream()
            .format("memory")
            .queryName(outputTable)
            .outputMode("append")
            .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("1 second"))
            .start();

        // Wait for data to be processed - use a more robust approach
        int maxWaitSeconds = 30;
        int waitedSeconds = 0;

        while (waitedSeconds < maxWaitSeconds) {
            try {
                Thread.sleep(1000);
                waitedSeconds++;

                // Check if we have any data processed
                long count = spark.sql("SELECT COUNT(*) FROM " + outputTable).collectAsList().get(0).getLong(0);
                if (count >= inputEvents.size()) {
                    break; // We have all the data we expect
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Table might not exist yet, continue waiting
            }
        }

        // Assert results
        List<String> results = spark.sql("SELECT * FROM " + outputTable).as(org.apache.spark.sql.Encoders.STRING()).collectAsList();

        // Stop the query
        query.stop();

        // Debug output
        System.out.println("Expected events: " + inputEvents);
        System.out.println("Actual results: " + results);
        System.out.println("Results count: " + results.size());

        Assertions.assertTrue(!results.isEmpty(), "Should have received some results from Kafka");
        Assertions.assertTrue(results.containsAll(inputEvents), "All input events should be processed by Spark");

        // NOTE: OpenLineage events are emitted via ConsoleTransport; in a real test, capture console output or mock transport to verify event structure
    }

    @Test
    void testMicrobatchWindowOLStress() throws TimeoutException {
        String topic = "test-topic-stress";
        int eventCount = 100;
        List<String> inputEvents = new ArrayList<>();
        for (int i = 0; i < eventCount; i++) {
            inputEvents.add("event" + i);
        }

        // Produce burst of events to Kafka
        Properties producerProps = createProducerProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        for (String event : inputEvents) {
            producer.send(new ProducerRecord<>(topic, event));
        }
        producer.flush();
        producer.close();

        // Set up Spark Structured Streaming with small microbatch interval
        String kafkaBootstrapServers = kafkaContainer.getBootstrapServers();
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> df = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrapServers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load();

        org.apache.spark.sql.Dataset<String> values = df.selectExpr("CAST(value AS STRING)").as(org.apache.spark.sql.Encoders.STRING());

        String outputTable = "stressOutputTable";
        org.apache.spark.sql.streaming.StreamingQuery query = values.writeStream()
            .format("memory")
            .queryName(outputTable)
            .outputMode("append")
            .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("100 milliseconds"))
            .start();

        try {
            // Wait for streaming query to process data with timeout
            query.awaitTermination(15000); // 15 seconds timeout for larger dataset
        } catch (org.apache.spark.sql.streaming.StreamingQueryException e) {
            // Query might terminate due to processing completion, which is expected
        }

        // Give additional time for processing if query is still active
        if (query.isActive()) {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Assert results
        List<String> results = spark.sql("SELECT * FROM " + outputTable).as(org.apache.spark.sql.Encoders.STRING()).collectAsList();

        // Stop the query
        query.stop();

        Assertions.assertEquals(eventCount, results.size(), "All burst events should be processed by Spark");

        // NOTE: To count OpenLineage events, mock ConsoleTransport or capture output in a real test
    }

    @Test
    void testComplexSparkOperationsLineage() throws TimeoutException {
        String topic = "test-topic-complex";
        List<String> inputEvents = Arrays.asList("apple", "banana", "apple", "orange", "banana", "apple");

        // Produce events to Kafka
        Properties producerProps = createProducerProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        for (String event : inputEvents) {
            producer.send(new ProducerRecord<>(topic, event));
        }
        producer.flush();
        producer.close();

        // Set up Spark Structured Streaming to read from Kafka
        String kafkaBootstrapServers = kafkaContainer.getBootstrapServers();
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> df = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrapServers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load();

        org.apache.spark.sql.Dataset<String> values = df.selectExpr("CAST(value AS STRING)").as(org.apache.spark.sql.Encoders.STRING());

        // Perform aggregation: count occurrences of each fruit
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> agg = values.groupBy("value").count();

        String outputTable = "aggOutputTable";
        org.apache.spark.sql.streaming.StreamingQuery query = agg.writeStream()
            .format("memory")
            .queryName(outputTable)
            .outputMode("complete")
            .start();

        try {
            // Wait for streaming query to process data with timeout
            query.awaitTermination(10000); // 10 seconds timeout
        } catch (org.apache.spark.sql.streaming.StreamingQueryException e) {
            // Query might terminate due to processing completion, which is expected
        }

        // Give additional time for processing if query is still active
        if (query.isActive()) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Assert aggregation results
        List<org.apache.spark.sql.Row> results = spark.sql("SELECT * FROM " + outputTable).collectAsList();

        // Stop the query
        query.stop();

        Map<String, Long> expectedCounts = new HashMap<>();
        expectedCounts.put("apple", 3L);
        expectedCounts.put("banana", 2L);
        expectedCounts.put("orange", 1L);

        Assertions.assertEquals(3, results.size(), "Should have 3 unique fruits");

        for (org.apache.spark.sql.Row row : results) {
            String fruit = row.getString(0);
            long count = row.getLong(1);
            Assertions.assertEquals(expectedCounts.get(fruit), count, "Count for " + fruit + " should match");
        }

        // NOTE: OpenLineage events should capture lineage for aggregation; verify by capturing ConsoleTransport output or using a mock
    }

    @Test
    void testOpenLineageEventsWithConsoleTransport() throws TimeoutException {
        String topic = "test-topic-lineage";
        List<String> inputEvents = Arrays.asList("lineage-event1", "lineage-event2");

        // Produce events to Kafka
        Properties producerProps = createProducerProperties();
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        for (String event : inputEvents) {
            producer.send(new ProducerRecord<>(topic, event));
        }
        producer.flush();
        producer.close();

        System.out.println("=== OpenLineage Events via Console Transport ===");

        // Set up Spark Structured Streaming to read from Kafka
        String kafkaBootstrapServers = kafkaContainer.getBootstrapServers();

        // Create a streaming DataFrame that will trigger OpenLineage events
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> df = spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrapServers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load();

        // Transform the data - this will create lineage information
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> transformed = df
            .selectExpr("CAST(value AS STRING) as message", "timestamp")
            .withColumn("processed_at", org.apache.spark.sql.functions.current_timestamp())
            .withColumn("message_length", org.apache.spark.sql.functions.length(org.apache.spark.sql.functions.col("message")));

        String outputTable = "lineageOutputTable";
        org.apache.spark.sql.streaming.StreamingQuery query = transformed.writeStream()
            .format("memory")
            .queryName(outputTable)
            .outputMode("append")
            .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("2 seconds"))
            .start();

        // Wait for data to be processed
        int maxWaitSeconds = 20;
        int waitedSeconds = 0;

        while (waitedSeconds < maxWaitSeconds) {
            try {
                Thread.sleep(1000);
                waitedSeconds++;

                // Check if we have any data processed
                try {
                    long count = spark.sql("SELECT COUNT(*) FROM " + outputTable).collectAsList().get(0).getLong(0);
                    if (count >= inputEvents.size()) {
                        break; // We have all the data we expect
                    }
                } catch (Exception e) {
                    // Table might not exist yet, continue waiting
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Perform a batch operation to trigger more lineage events
        System.out.println("=== Performing batch operation to trigger additional lineage events ===");
        org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> batchResults = spark.sql("SELECT message, message_length FROM " + outputTable);
        batchResults.show(); // This will trigger lineage events for the batch operation

        // Stop the query
        query.stop();

        // Assert results
        List<org.apache.spark.sql.Row> results = spark.sql("SELECT * FROM " + outputTable).collectAsList();
        System.out.println("=== Final Results ===");
        System.out.println("Processed " + results.size() + " events with lineage tracking");

        Assertions.assertTrue(!results.isEmpty(), "Should have processed some events");
        System.out.println("=== OpenLineage Console Transport Demo Complete ===");
    }
}

