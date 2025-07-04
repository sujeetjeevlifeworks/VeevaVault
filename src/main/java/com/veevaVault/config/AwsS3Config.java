package com.veevaVault.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
public class AwsS3Config {
    public String getEncryptedAccessKey() {
        return encryptedAccessKey;
    }

    public String getEncryptedSecretKey() {
        return encryptedSecretKey;
    }

    public String getRegion() {
        return region;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getAthenaOutputS3() {
        return athenaOutputS3;
    }

    @Value("${aws.access-key-encrypted}")
    private String encryptedAccessKey;

    @Value("${aws.secret-key-encrypted}")
    private String encryptedSecretKey;

    @Value("${aws.region}")
    private String region;

    @Value("${aws.s3.bucket}")
    private String bucketName;

    @Value("${aws.athena.output-s3}")
    private String athenaOutputS3;
}