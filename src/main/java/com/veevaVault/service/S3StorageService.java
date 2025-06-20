package com.veevaVault.service;
import com.veevaVault.config.AwsS3Config;
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
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.Base64;

@Service
public class S3StorageService {

    private static final String initVector = "123456$#@$^@1ERF";
    private static final String key = "123456$#@$^@1ERF";

    private final S3Client s3Client;
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
        String s3Key = String.format("vault-data/extract_type=%s/load_date=%s/%s", extractType, loadDate, fileName);

        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        s3Client.putObject(putRequest, RequestBody.fromBytes(fileData));
        System.out.println("✅ Uploaded to S3: " + s3Key);
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
            ex.printStackTrace();
            return null;
        }
    }
}