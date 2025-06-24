package com.veevaVault.service;

import com.veevaVault.config.AwsS3Config;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class AthenaService {
    private static final String CATALOG_NAME = "AwsDataCatalog";
    private static final String DATABASE_NAME = "veeva_vault_db";
    private static final Duration QUERY_TIMEOUT = Duration.ofMinutes(5);
    private static final String CSV_SERDE = "org.apache.hadoop.hive.serde2.OpenCSVSerde";

    private final AthenaClient athenaClient;
    private final GlueClient glueClient;
    private final S3Client s3Client;
    private final String outputLocation;
    private final String bucketName;
    private final String glueServiceRoleArn;

    public AthenaService(S3StorageService s3StorageService, AwsS3Config awsS3Config) {
        this.bucketName = awsS3Config.getBucketName();
        this.outputLocation = awsS3Config.getAthenaOutputS3();
        this.glueServiceRoleArn = awsS3Config.getGlueServiceRoleArn();

        AwsBasicCredentials credentials = AwsBasicCredentials.create(
                S3StorageService.decrypt(awsS3Config.getEncryptedAccessKey()),
                S3StorageService.decrypt(awsS3Config.getEncryptedSecretKey())
        );

        Region region = Region.of(awsS3Config.getRegion());

        this.athenaClient = AthenaClient.builder()
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .build();

        this.glueClient = GlueClient.builder()
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .build();

        this.s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .build();
    }

    @PostConstruct
    public void initialize() {
        ensureDatabaseExists();
    }

    private void ensureDatabaseExists() {
        try {
            glueClient.getDatabase(software.amazon.awssdk.services.glue.model.GetDatabaseRequest.builder()
                    .name(DATABASE_NAME)
                    .build());
        } catch (Exception e) {
            // Database doesn't exist, create it
            glueClient.createDatabase(software.amazon.awssdk.services.glue.model.CreateDatabaseRequest.builder()
                    .databaseInput(DatabaseInput.builder()
                            .name(DATABASE_NAME)
                            .description("Database for Veeva Vault extracted data")
                            .build()).build());
        }
    }

    // ... [rest of the class implementation remains the same]


    public void createOrUpdateTable(String extractDirName) {
        String tableName = normalizeTableName(extractDirName);
        String s3Location = "s3://" + bucketName + "/objects/" + extractDirName + "/";

        try {
            if (!tableExists(tableName)) {
                createNewTable(tableName, s3Location);
                System.out.println("🆕 Created Athena table: " + tableName);
            } else {
                updateTablePartitions(tableName, s3Location);
                System.out.println("🔄 Updated Athena table partitions: " + tableName);

                updateTableSchemaIfNeeded(tableName, extractDirName);
            }
        } catch (Exception e) {
            System.err.println("❌ Failed to process table " + tableName + ": " + e.getMessage());
            throw new RuntimeException("Failed to process Athena table", e);
        }
    }

    private boolean tableExists(String tableName) {
        try {
            glueClient.getTable(GetTableRequest.builder()
                    .databaseName(DATABASE_NAME)
                    .name(tableName)
                    .build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private String normalizeTableName(String extractDirName) {
        return extractDirName.toLowerCase()
                .replaceAll("[^a-z0-9_]", "_")
                .replaceAll("_+", "_")
                .replaceAll("^_|_$", "");
    }

    private void createNewTable(String tableName, String s3Location) {
        try {
            Optional<TableSchema> inferredSchema = inferSchemaFromS3(tableName);
            if (inferredSchema.isPresent()) {
                createTableWithInferredSchema(tableName, s3Location, inferredSchema.get());
                return;
            }
        } catch (Exception e) {
            System.err.println("Schema inference failed, falling back to default table creation: " + e.getMessage());
        }

        // Fallback to generic table creation
        String createQuery = String.format(
                "CREATE EXTERNAL TABLE `%s`.`%s` (\n" +
                        "  `column1` string,\n" +
                        "  `column2` string\n" +
                        ")\n" +
                        "PARTITIONED BY (extract_date string)\n" +
                        "ROW FORMAT SERDE '%s'\n" +
                        "WITH SERDEPROPERTIES (\n" +
                        "  'separatorChar' = ',',\n" +
                        "  'quoteChar' = '\"',\n" +
                        "  'escapeChar' = '\\\\',\n" +
                        "  'skip.header.line.count' = '1'\n" +
                        ")\n" +
                        "STORED AS TEXTFILE\n" +
                        "LOCATION '%s'\n" +
                        "TBLPROPERTIES (\n" +
                        "  'has_encrypted_data'='false',\n" +
                        "  'projection.enabled'='true',\n" +
                        "  'projection.extract_date.type'='date',\n" +
                        "  'projection.extract_date.format'='yyyy-MM-dd',\n" +
                        "  'projection.extract_date.range'='NOW-3YEARS,NOW',\n" +
                        "  'storage.location.template'='%s/${extract_date}/'\n" +
                        ")",
                DATABASE_NAME, tableName, CSV_SERDE, s3Location, s3Location);

        executeAthenaQuery(createQuery);
        executeAthenaQuery(String.format("MSCK REPAIR TABLE `%s`.`%s`", DATABASE_NAME, tableName));
    }

    private Optional<TableSchema> inferSchemaFromS3(String tableName) {
        try {
            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix("objects/" + tableName + "/")
                    .build();

            Optional<S3Object> sampleFile = s3Client.listObjectsV2(listRequest).contents().stream()
                    .filter(s3Object -> s3Object.key().endsWith(".csv"))
                    .findFirst();

            if (sampleFile.isPresent()) {
                String sampleFileLocation = "s3://" + bucketName + "/" + sampleFile.get().key();
                String tempTableName = "temp_schema_inference_" + System.currentTimeMillis();

                String createTempTableQuery = String.format(
                        "CREATE EXTERNAL TABLE `%s`.`%s` (\n" +
                                "  `dummy` string\n" +
                                ")\n" +
                                "ROW FORMAT SERDE '%s'\n" +
                                "WITH SERDEPROPERTIES (\n" +
                                "  'separatorChar' = ',',\n" +
                                "  'quoteChar' = '\"',\n" +
                                "  'escapeChar' = '\\\\',\n" +
                                "  'skip.header.line.count' = '1'\n" +
                                ")\n" +
                                "STORED AS TEXTFILE\n" +
                                "LOCATION '%s'\n",
                        DATABASE_NAME, tempTableName, CSV_SERDE, sampleFileLocation);

                executeAthenaQuery(createTempTableQuery);

                GetTableResponse tableResponse = glueClient.getTable(GetTableRequest.builder()
                        .databaseName(DATABASE_NAME)
                        .name(tempTableName)
                        .build());

                executeAthenaQuery(String.format("DROP TABLE `%s`.`%s`", DATABASE_NAME, tempTableName));

                return Optional.of(new TableSchema(tableResponse.table().storageDescriptor().columns()));
            }
        } catch (Exception e) {
            System.err.println("Failed to infer schema: " + e.getMessage());
        }
        return Optional.empty();
    }

    private void createTableWithInferredSchema(String tableName, String s3Location, TableSchema schema) {
        String columns = schema.columns.stream()
                .map(col -> String.format("  `%s` %s", col.name, "string"))
                .collect(Collectors.joining(",\n"));

        String createQuery = String.format(
                "CREATE EXTERNAL TABLE `%s`.`%s` (\n" +
                        "%s\n" +
                        ")\n" +
                        "PARTITIONED BY (extract_date string)\n" +
                        "ROW FORMAT SERDE '%s'\n" +
                        "WITH SERDEPROPERTIES (\n" +
                        "  'separatorChar' = ',',\n" +
                        "  'quoteChar' = '\"',\n" +
                        "  'escapeChar' = '\\\\',\n" +
                        "  'skip.header.line.count' = '1'\n" +
                        ")\n" +
                        "STORED AS TEXTFILE\n" +
                        "LOCATION '%s'\n" +
                        "TBLPROPERTIES (\n" +
                        "  'has_encrypted_data'='false',\n" +
                        "  'projection.enabled'='true',\n" +
                        "  'projection.extract_date.type'='date',\n" +
                        "  'projection.extract_date.format'='yyyy-MM-dd',\n" +
                        "  'projection.extract_date.range'='NOW-3YEARS,NOW',\n" +
                        "  'storage.location.template'='%s/${extract_date}/'\n" +
                        ")",
                DATABASE_NAME, tableName, columns, CSV_SERDE, s3Location, s3Location);

        executeAthenaQuery(createQuery);
        executeAthenaQuery(String.format("MSCK REPAIR TABLE `%s`.`%s`", DATABASE_NAME, tableName));
    }

    private void updateTablePartitions(String tableName, String s3Location) {
        executeAthenaQuery(String.format("MSCK REPAIR TABLE `%s`.`%s`", DATABASE_NAME, tableName));
    }

    private void updateTableSchemaIfNeeded(String tableName, String extractDirName) {
        try {
            Optional<TableSchema> currentSchema = getCurrentTableSchema(tableName);
            Optional<TableSchema> newSchema = inferSchemaFromS3(extractDirName);

            if (currentSchema.isPresent() && newSchema.isPresent() &&
                    !schemasMatch(currentSchema.get(), newSchema.get())) {
                System.out.println("🔄 Schema change detected, updating table: " + tableName);
                updateTableSchema(tableName, extractDirName, newSchema.get());
            }
        } catch (Exception e) {
            System.err.println("Failed to check/update schema: " + e.getMessage());
        }
    }

    private boolean schemasMatch(TableSchema schema1, TableSchema schema2) {
        if (schema1.columns.size() != schema2.columns.size()) {
            return false;
        }

        for (int i = 0; i < schema1.columns.size(); i++) {
            if (!schema1.columns.get(i).name.equalsIgnoreCase(schema2.columns.get(i).name)) {
                return false;
            }
        }

        return true;
    }

    private Optional<TableSchema> getCurrentTableSchema(String tableName) {
        try {
            GetTableResponse response = glueClient.getTable(GetTableRequest.builder()
                    .databaseName(DATABASE_NAME)
                    .name(tableName)
                    .build());

            return Optional.of(new TableSchema(response.table().storageDescriptor().columns()));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private void updateTableSchema(String tableName, String extractDirName, TableSchema newSchema) {
        String tempTableName = tableName + "_new_" + System.currentTimeMillis();
        String s3Location = "s3://" + bucketName + "/objects/" + extractDirName + "/";

        createTableWithInferredSchema(tempTableName, s3Location, newSchema);

        String copyQuery = String.format(
                "INSERT INTO `%s`.`%s` " +
                        "SELECT * FROM `%s`.`%s`",
                DATABASE_NAME, tempTableName,
                DATABASE_NAME, tableName);

        executeAthenaQuery(copyQuery);
        executeAthenaQuery(String.format("DROP TABLE `%s`.`%s`", DATABASE_NAME, tableName));
        executeAthenaQuery(String.format(
                "ALTER TABLE `%s`.`%s` RENAME TO `%s`",
                DATABASE_NAME, tempTableName, tableName));
    }

    private void executeAthenaQuery(String query) {
        String queryExecutionId = startQueryExecution(query);
        waitForQueryToComplete(queryExecutionId);
    }

    private String startQueryExecution(String query) {
        QueryExecutionContext context = QueryExecutionContext.builder()
                .database(DATABASE_NAME)
                .catalog(CATALOG_NAME)
                .build();

        ResultConfiguration resultConfig = ResultConfiguration.builder()
                .outputLocation(outputLocation)
                .build();

        StartQueryExecutionRequest request = StartQueryExecutionRequest.builder()
                .queryString(query)
                .queryExecutionContext(context)
                .resultConfiguration(resultConfig)
                .build();

        StartQueryExecutionResponse response = athenaClient.startQueryExecution(request);
        return response.queryExecutionId();
    }

    private void waitForQueryToComplete(String queryExecutionId) {
        Instant startTime = Instant.now();

        while (true) {
            GetQueryExecutionRequest request = GetQueryExecutionRequest.builder()
                    .queryExecutionId(queryExecutionId)
                    .build();

            GetQueryExecutionResponse response = athenaClient.getQueryExecution(request);
            QueryExecutionState state = response.queryExecution().status().state();

            if (state == QueryExecutionState.FAILED) {
                throw new RuntimeException("Query failed: " +
                        response.queryExecution().status().stateChangeReason());
            } else if (state == QueryExecutionState.CANCELLED) {
                throw new RuntimeException("Query was cancelled");
            } else if (state == QueryExecutionState.SUCCEEDED) {
                return;
            }

            if (Duration.between(startTime, Instant.now()).compareTo(QUERY_TIMEOUT) > 0) {
                throw new RuntimeException("Query timed out");
            }

            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for query to complete");
            }
        }
    }

    private static class TableSchema {
        private final List<Column> columns;

        public TableSchema(List<?> columns) {
            if (columns == null || columns.isEmpty()) {
                this.columns = Collections.emptyList();
                return;
            }

            if (columns.get(0) instanceof software.amazon.awssdk.services.glue.model.Column) {
                this.columns = ((List<software.amazon.awssdk.services.glue.model.Column>) columns).stream()
                        .map(col -> new Column(col.name(), col.type()))
                        .collect(Collectors.toList());
            } else {
                this.columns = (List<Column>) columns;
            }
        }
    }

    private static class Column {
        private final String name;
        private final String type;

        public Column(String name, String type) {
            this.name = name;
            this.type = type;
        }
    }
    public void createTablesForAllCsvFiles() {
        ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix("objects/")
                .delimiter("/")
                .build();

        s3Client.listObjectsV2(listRequest).commonPrefixes().forEach(prefix -> {
            String folder = prefix.prefix(); // e.g., objects/case_volume/
            String dirName = folder.replace("objects/", "").replace("/", ""); // e.g., case_volume

            ListObjectsV2Request csvCheck = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(folder)
                    .build();

            boolean hasCsv = s3Client.listObjectsV2(csvCheck).contents().stream()
                    .anyMatch(obj -> obj.key().endsWith(".csv"));

            if (hasCsv) {
                System.out.println("📄 Found CSV in: " + folder);
                createOrUpdateTable(dirName);
            } else {
                System.out.println("⚠️ No CSV in: " + folder);
            }
        });
    }

}