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
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
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
    private static final String DATABASE_NAME = "veeva_vault_db";
    private static final String CSV_SERDE = "org.apache.hadoop.hive.serde2.OpenCSVSerde";

    private final AthenaClient athenaClient;
    private final GlueClient glueClient;
    private final S3Client s3Client;
    private final String outputLocation;
    private final String bucketName;

    public AthenaService(S3StorageService s3StorageService, AwsS3Config config) {
        this.bucketName = config.getBucketName();
        this.outputLocation = config.getAthenaOutputS3();

        AwsBasicCredentials credentials = AwsBasicCredentials.create(
                S3StorageService.decrypt(config.getEncryptedAccessKey()),
                S3StorageService.decrypt(config.getEncryptedSecretKey())
        );

        Region region = Region.of(config.getRegion());

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

        ensureDatabaseExists();
    }

    private void ensureDatabaseExists() {
        try {
            glueClient.getDatabase(GetDatabaseRequest.builder().name(DATABASE_NAME).build());
        } catch (Exception e) {
            glueClient.createDatabase(CreateDatabaseRequest.builder()
                    .databaseInput(DatabaseInput.builder()
                            .name(DATABASE_NAME)
                            .description("Veeva Vault Extracted Data")
                            .build())
                    .build());
        }
    }

    public void createOrUpdateTable(String folderName) {
        String tableName = folderName.toLowerCase().replaceAll("[^a-z0-9]", "_");
        String s3Location = "s3://" + bucketName + "/objects/" + folderName + "/";

        if (!tableExists(tableName)) {
            String query = String.format(
                    "CREATE EXTERNAL TABLE `%s`.`%s` (\n" +
                            "  `col1` string,\n" +
                            "  `col2` string\n" + // adjust this if needed
                            ")\n" +
                            "ROW FORMAT SERDE '%s'\n" +
                            "WITH SERDEPROPERTIES (\n" +
                            "  'separatorChar' = ',',\n" +
                            "  'quoteChar' = '\"',\n" +
                            "  'escapeChar' = '\\\\',\n" +
                            "  'skip.header.line.count' = '1'\n" +
                            ")\n" +
                            "STORED AS TEXTFILE\n" +
                            "LOCATION '%s'",
                    DATABASE_NAME, tableName, CSV_SERDE, s3Location
            );

            executeAthenaQuery(query);
            System.out.println("✅ Athena table created: " + tableName);
        } else {
            System.out.println("ℹ️ Table already exists: " + tableName);
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

    private void executeAthenaQuery(String query) {
        QueryExecutionContext context = QueryExecutionContext.builder()
                .database(DATABASE_NAME)
                .build();

        ResultConfiguration resultConfig = ResultConfiguration.builder()
                .outputLocation(outputLocation)
                .build();

        StartQueryExecutionResponse response = athenaClient.startQueryExecution(StartQueryExecutionRequest.builder()
                .queryString(query)
                .queryExecutionContext(context)
                .resultConfiguration(resultConfig)
                .build());

        String queryExecutionId = response.queryExecutionId();

        // Basic wait
        while (true) {
            QueryExecutionState state = athenaClient.getQueryExecution(GetQueryExecutionRequest.builder()
                            .queryExecutionId(queryExecutionId)
                            .build())
                    .queryExecution()
                    .status()
                    .state();

            if (state == QueryExecutionState.SUCCEEDED) return;
            if (state == QueryExecutionState.FAILED || state == QueryExecutionState.CANCELLED)
                throw new RuntimeException("Athena query failed: " + state);
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted");
            }
        }
    }
}

