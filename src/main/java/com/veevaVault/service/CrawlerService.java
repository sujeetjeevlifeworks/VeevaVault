package com.veevaVault.service;

import com.veevaVault.config.AwsS3Config;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

import java.util.List;

@Service
public class CrawlerService {
    private static final String DATABASE_NAME = "veeva_vault_db";
    private static final String CRAWLER_ROLE = "AWSGlueServiceRole";

    private final GlueClient glueClient;
    private final String bucketName;

    public CrawlerService(AwsS3Config config) {
        this.bucketName = config.getBucketName();

        AwsBasicCredentials credentials = AwsBasicCredentials.create(
                S3StorageService.decrypt(config.getEncryptedAccessKey()),
                S3StorageService.decrypt(config.getEncryptedSecretKey())
        );

        this.glueClient = GlueClient.builder()
                .region(Region.of(config.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .build();

        ensureDatabaseExists();
    }

    private void ensureDatabaseExists() {
        try {
            glueClient.getDatabase(GetDatabaseRequest.builder()
                    .name(DATABASE_NAME)
                    .build());
        } catch (Exception e) {
            glueClient.createDatabase(CreateDatabaseRequest.builder()
                    .databaseInput(DatabaseInput.builder()
                            .name(DATABASE_NAME)
                            .description("Database for Veeva Vault extracted data")
                            .build())
                    .build());
        }
    }

    public void createOrUpdateCrawler(String folderName) {
        String crawlerName = "veeva_crawler_" + folderName.toLowerCase().replaceAll("[^a-z0-9]", "_");
        String s3Path = "s3://" + bucketName + "/objects/" + folderName + "/";

        try {
            // Try to get existing crawler
            GetCrawlerResponse response = glueClient.getCrawler(GetCrawlerRequest.builder()
                    .name(crawlerName)
                    .build());

            // Update existing crawler
            glueClient.updateCrawler(UpdateCrawlerRequest.builder()
                    .name(crawlerName)
                    .targets(CrawlerTargets.builder()
                            .s3Targets(S3Target.builder().path(s3Path).build())
                            .build())
                    .role(CRAWLER_ROLE)
                    .build());
        } catch (Exception e) {
            // Create new crawler if it doesn't exist
            glueClient.createCrawler(CreateCrawlerRequest.builder()
                    .name(crawlerName)
                    .targets(CrawlerTargets.builder()
                            .s3Targets(S3Target.builder().path(s3Path).build())
                            .build())
                    .role(CRAWLER_ROLE)
                    .databaseName(DATABASE_NAME)
                    .build());
        }

        // Start the crawler
        glueClient.startCrawler(StartCrawlerRequest.builder()
                .name(crawlerName)
                .build());
    }

    public void createCrawlersForAllFolders() {
        // List all crawlers
        ListCrawlersResponse crawlersResponse = glueClient.listCrawlers(ListCrawlersRequest.builder().build());

        // Filter and start our crawlers
        crawlersResponse.crawlerNames().forEach(crawlerName -> {
            if (crawlerName.startsWith("veeva_crawler_")) {
                glueClient.startCrawler(StartCrawlerRequest.builder()
                        .name(crawlerName)
                        .build());
            }
        });
    }
}