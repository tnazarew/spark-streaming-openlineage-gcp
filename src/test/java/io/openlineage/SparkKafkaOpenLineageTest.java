package io.openlineage;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Durations;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class SparkKafkaOpenLineageTest {
    protected KafkaContainer kafkaContainer;
    protected SparkSession spark;
    JavaStreamingContext streamingContext;

    private static final String PROPERTIES_FILE = "openlineage-gcp.properties";

    private String projectId;
    private String location;
    private String credentialsFile;


    @BeforeAll
    @SuppressWarnings("deprecation")
    void setup() throws IOException {
        // Load configuration from properties file
        loadOpenLineageConfig();

        // Start Kafka container
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));
        kafkaContainer.start();
        spark = SparkSession.builder()
                .appName(getTestName())
                .master("local[2]")
                .config("spark.sql.warehouse.dir", "spark-warehouse")
                .config("javax.jdo.option.ConnectionURL", "jdbc:derby:memory:metastore_db;create=true")
                .config("javax.jdo.option.ConnectionDriverName", "org.apache.derby.jdbc.EmbeddedDriver")
                .config("spark.sql.catalogImplementation", "hive")
                .config("spark.driver.host", "localhost")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

                // OpenLineage Spark integration configuration - use config file
                .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
                .config("spark.openlineage.namespace", "test")
                .config( "spark.openlineage.transport.type", "composite")
                .config("spark.openlineage.transport.continueOnFailure","true")
                .config("spark.openlineage.transport.transports.my_file.type", "file")
                .config("spark.openlineage.transport.transports.my_file.location", String.format("events/%s.json", getTestName()))
                .config("spark.openlineage.transport.transports.my_gcp.type", "gcplineage")
                .config("spark.openlineage.transport.transports.my_gcp.projectId", projectId)
                .config("spark.openlineage.transport.transports.my_gcp.location", location)
                .config("spark.openlineage.transport.transports.my_gcp.mode", "ASYNC")
                .config("spark.openlineage.transport.transports.my_gcp.credentialsFile", credentialsFile)

                .getOrCreate();

        // Create JavaSparkContext from SparkSession
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        streamingContext = new JavaStreamingContext(jsc, Durations.seconds(1));

    }

    // Load OpenLineage GCP transport configuration from properties
    private void loadOpenLineageConfig() throws IOException {
        Properties props = new Properties();
        props.load(getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE));

        // Validation: all required keys must be present and non-empty
        List<String> missing = Stream.of("ol.gcp.projectId", "ol.gcp.location", "ol.gcp.credentialsFile")
                .filter(k -> props.getProperty(k) == null || props.getProperty(k).trim().isEmpty())
                .collect(Collectors.toList());
        if (!missing.isEmpty()) {
            String help = "Missing or empty OpenLineage GCP config: " + missing + "\n" +
                    "Set non-empty values in '" + PROPERTIES_FILE + "'.";
            throw new RuntimeException(help);
        }

        // Assign validated values
        projectId = props.getProperty("ol.gcp.projectId").trim();
        location = props.getProperty("ol.gcp.location").trim();
        credentialsFile = props.getProperty("ol.gcp.credentialsFile").trim();
    }

    @AfterAll
    void teardown() {
        streamingContext.stop(true, true);
        spark.stop();
        kafkaContainer.stop();
    }

    // Remove any existing per-test events file before each test runs
    @BeforeEach
    void removeEventFileBeforeTest() {
        try {
            Path eventFile = Paths.get("events", String.format("%s.json", getTestName()));
            if (Files.exists(eventFile)) {
                Files.deleteIfExists(eventFile);
            }
        } catch (Exception e) {
            // best-effort; log and continue
            System.err.println("Failed to delete event file before test: " + e.getMessage());
        }
    }

    // Central cleanup executed after each test to stop streaming queries, drop tables, and delete test dirs
    @AfterEach
    void cleanupAfterEach() {
        if (spark == null) {
            return;
        }

        // Stop any active streaming queries
        try {
            org.apache.spark.sql.streaming.StreamingQuery[] active = spark.streams().active();
            if (active != null) {
                for (org.apache.spark.sql.streaming.StreamingQuery q : active) {
                    try {
                        if (q != null && q.isActive()) {
                            q.stop();
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to stop streaming query during cleanup: " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            // ignore
            System.err.println("Error enumerating streaming queries: " + e.getMessage());
        }

        // Drop any non-temporary tables in the catalog (best-effort)
        try {
            List<org.apache.spark.sql.catalog.Table> tables = spark.catalog().listTables().collectAsList();
            for (org.apache.spark.sql.catalog.Table r : tables) {
                try {
                    String name = r.name();
                    if (name != null && !name.isEmpty()) {
                        spark.sql("DROP TABLE IF EXISTS " + name);
                    }
                } catch (Exception e) {
                    System.err.println("Failed to drop table during cleanup: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            // ignore
            System.err.println("Error listing tables for cleanup: " + e.getMessage());
        }

        // Best-effort deletion of common directories used by tests
        deleteDirectoryRecursive("spark-warehouse");
        deleteDirectoryRecursive("spark-warehouse/spark-warehouse");
        deleteDirectoryRecursive("checkpoint");

        // (Intentionally no deletion of events/<testName>.json here — event files should only be removed before tests)
    }

    // Utility to delete a directory recursively using NIO
    private void deleteDirectoryRecursive(String dirPath) {
        // Build candidate paths to try: as given, user.dir + dirPath, and nested spark-warehouse variants
        String userDir = System.getProperty("user.dir");
        java.util.List<Path> candidates = new java.util.ArrayList<>();
        candidates.add(Paths.get(dirPath));
        candidates.add(Paths.get(userDir, dirPath));

        // If dirPath contains spark-warehouse already, also try alternate nestings
        String tableName = Paths.get(dirPath).getFileName().toString();
        candidates.add(Paths.get(userDir, "spark-warehouse", tableName));
        candidates.add(Paths.get(userDir, "spark-warehouse", "spark-warehouse", tableName));

        for (Path dir : candidates) {
            if (Files.exists(dir)) {
                try (Stream<Path> stream = Files.walk(dir)) {
                    stream.sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(f -> {
                                if (!f.delete()) {
                                    System.err.println("Failed to delete file during cleanup: " + f.getAbsolutePath());
                                }
                            });
                } catch (IOException e) {
                    // try next candidate
                    System.err.println("Failed to delete candidate directory " + dir + ": " + e.getMessage());
                }
                // if deleted, break out
                if (!Files.exists(dir)) {
                    return;
                }
            }
        }
    }

    protected Properties createProducerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaContainer.getBootstrapServers());
        props.put("key.serializer", getKeySerializer());
        props.put("value.serializer", getValueSerializer());
        return props;
    }

    // Common helpers for tests
    protected void registerCustomersView(String csvPath) {
        spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csvPath)
                .createOrReplaceTempView("customers");
    }

    protected String getDescriptorPath(String resourceName) {
        try {
            java.net.URL resource = getClass().getClassLoader().getResource(resourceName);
            if (resource == null) {
                throw new RuntimeException("Resource not found: " + resourceName);
            }
            return java.nio.file.Paths.get(resource.toURI()).toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to resolve descriptor path for " + resourceName + ": " + e.getMessage(), e);
        }
    }

    protected org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> readKafkaStream(String topic, boolean failOnDataLoss) {
        org.apache.spark.sql.streaming.DataStreamReader reader = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", kafkaContainer.getBootstrapServers())
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .option("failOnDataLoss", String.valueOf(failOnDataLoss));
        return reader.load();
    }

    protected abstract String getTestName();
    protected abstract String getKeySerializer();
    protected abstract String getValueSerializer();
}
