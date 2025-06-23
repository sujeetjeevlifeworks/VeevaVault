package com.veevaVault.service;

import com.veevaVault.config.AwsS3Config;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.athena.model.*;

import java.time.LocalDate;

@Service
public class AthenaService {

    private AthenaClient athenaClient;
    private final String outputLocation;
    private final String databaseName = "veeva_vault_db";

    private final S3StorageService s3StorageService;
    private final AwsS3Config awsS3Config;

    public AthenaService(S3StorageService s3StorageService, AwsS3Config awsS3Config) {
        this.s3StorageService = s3StorageService;
        this.awsS3Config = awsS3Config;
        this.outputLocation = awsS3Config.getAthenaOutputS3();
    }

    @PostConstruct
    public void init() {
        AwsBasicCredentials credentials = AwsBasicCredentials.create(
                S3StorageService.decrypt(awsS3Config.getEncryptedAccessKey()),
                S3StorageService.decrypt(awsS3Config.getEncryptedSecretKey())
        );

        this.athenaClient = AthenaClient.builder()
                .region(Region.of(awsS3Config.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .build();
    }

    public void createOrUpdateTable(String extractType) {
        String loadDate = LocalDate.now().toString();
        String s3Location = String.format("s3://%s/vault-data/extract_type=%s/load_date=%s/",
                s3StorageService.getBucketName(), extractType, loadDate);

        String tableName = extractType.replaceAll("_", "") + "_table";

        String query = String.format("""
                CREATE EXTERNAL TABLE IF NOT EXISTS %s (
                    id string,
                    name string,
                    value string
                )
                ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                WITH SERDEPROPERTIES ('serialization.format' = ',', 'field.delim' = ',')
                LOCATION '%s'
                TBLPROPERTIES ('has_encrypted_data'='false')
                """, tableName, s3Location);

        startQueryExecution(query);
    }

    private void startQueryExecution(String query) {
        StartQueryExecutionRequest request = StartQueryExecutionRequest.builder()
                .queryString(query)
                .queryExecutionContext(QueryExecutionContext.builder().database(databaseName).build())
                .resultConfiguration(ResultConfiguration.builder().outputLocation(outputLocation).build())
                .build();

        StartQueryExecutionResponse response = athenaClient.startQueryExecution(request);
        System.out.println("✅ Athena query started: " + response.queryExecutionId());
    }
}
