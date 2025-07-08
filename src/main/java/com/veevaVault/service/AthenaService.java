package com.veevaVault.service;

import com.veevaVault.config.AwsS3Config;
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
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

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

    public void createOrUpdateTable(String folderName, String fileName) {
        String tableName = folderName.toLowerCase().replaceAll("[^a-z0-9]", "_");
        String s3Location = "s3://" + bucketName + "/objects/" + folderName + "/";
        String s3Key = "objects/" + folderName + "/" + fileName;

        String headerLine;
        try (InputStream stream = s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build());
             BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {

            headerLine = reader.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                System.out.println("Header is empty for file: " + s3Key);
                return;
            }

        } catch (Exception e) {
            System.out.println("Failed to read header from file: " + s3Key + " - " + e.getMessage());
            return;
        }

        // Extract columns
        String[] columns = headerLine.split(",");
        StringBuilder columnsDef = new StringBuilder();
        for (String col : columns) {
            String cleanCol = col.trim()
                    .toLowerCase()
                    .replaceAll("[^a-z0-9_]", "_");
            if (!cleanCol.isBlank()) {
                columnsDef.append("`").append(cleanCol).append("` string,\n");
            }
        }
        if (columnsDef.length() > 0) {
            columnsDef.setLength(columnsDef.length() - 2); // remove last comma
        }

        if (!tableExists(tableName)) {
            String query = String.format("""
                CREATE EXTERNAL TABLE `%s`.`%s` (
                %s
                )
                ROW FORMAT SERDE '%s'
                WITH SERDEPROPERTIES (
                    'separatorChar' = ',',
                    'quoteChar' = '\"',
                    'escapeChar' = '\\\\'
                )
                STORED AS TEXTFILE
                LOCATION '%s'
                TBLPROPERTIES ('skip.header.line.count'='1')
                """,
                    DATABASE_NAME, tableName, columnsDef, CSV_SERDE, s3Location);

            executeAthenaQuery(query);
            System.out.println(" Created Athena table: " + tableName);
        } else {
            System.out.println("Athena table already exists: " + tableName);
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
