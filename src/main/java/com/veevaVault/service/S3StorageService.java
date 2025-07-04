
package com.veevaVault.service;
import com.veevaVault.config.AwsS3Config;
import lombok.Getter;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
public class S3StorageService {
    private static final Logger log = LoggerFactory.getLogger(S3StorageService.class);
    private static final String initVector = "123456$#@$^@1ERF";
    private static final String key = "123456$#@$^@1ERF";

    private final S3Client s3Client;
    @Getter
    private final String bucketName;

    @Autowired
    public S3StorageService(AwsS3Config config) {
        String accessKey = decrypt(config.getEncryptedAccessKey());
        String secretKey = decrypt(config.getEncryptedSecretKey());

        this.bucketName = config.getBucketName();

        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);

        this.s3Client = S3Client.builder()
                .region(Region.of(config.getRegion()))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
    }

    public void uploadToS3(byte[] fileData, String fileName, String extractType) {
        String loadDate = LocalDate.now().toString();
        String s3Key = "objects/" + fileName;

        try {
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();

            s3Client.putObject(putRequest, RequestBody.fromBytes(fileData));
            log.info("Successfully uploaded to S3: {}", s3Key);
        } catch (Exception e) {
            log.error("Failed to upload {} to S3: {}", s3Key, e.getMessage());
            throw new RuntimeException("S3 upload failed", e);
        }
    }
    public void uploadToS3(InputStream inputStream, String fileName, String extractType, long contentLength) {
        String s3Key = "objects/" + fileName;

        try {
            PutObjectRequest putRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();

            s3Client.putObject(putRequest, RequestBody.fromInputStream(inputStream, contentLength));
            log.info(" Stream uploaded to S3: {}", s3Key);
        } catch (Exception e) {
            log.error(" Failed to upload stream to S3: {}", s3Key, e.getMessage());
            throw new RuntimeException("S3 stream upload failed", e);
        }
    }

    public static String decrypt(String encrypted) {
        try {
            IvParameterSpec iv = new IvParameterSpec(initVector.getBytes(StandardCharsets.UTF_8));
            SecretKeySpec skeySpec = new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), "AES");

            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
            cipher.init(Cipher.DECRYPT_MODE, skeySpec, iv);

            byte[] original = cipher.doFinal(Base64.getDecoder().decode(encrypted));
            return new String(original, StandardCharsets.UTF_8);
        } catch (Exception ex) {
            log.error("Decryption failed", ex);
            throw new RuntimeException("Decryption failed", ex);
        }
    }

}