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
import java.nio.file.Paths;
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
    private static final Logger logger = LoggerFactory.getLogger(S3StorageService.class);

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
   /* public static List<File> extractCsvFromBinary(byte[] fileData, String fileName) {
        try {
            // Define temp file path
            File csvFile = File.createTempFile(fileName.replace(".001", ""), ".csv");

            try (GZIPInputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(fileData));
                 FileOutputStream fileOutputStream = new FileOutputStream(csvFile)) {

                byte[] buffer = new byte[4096];
                int len;
                while ((len = gzipInputStream.read(buffer)) > 0) {
                    fileOutputStream.write(buffer, 0, len);
                }
            }

            return List.of(csvFile);

        } catch (IOException e) {
            throw new RuntimeException("❌ Failed to extract CSV from GZIP: " + fileName, e);
        }
    }
*/


    public void uploadToS3(byte[] fileData, String fileName, String extractType) {
        String loadDate = LocalDate.now().toString();
        //String s3Key = String.format("vault-data/extract_type=%s/load_date=%s/%s", extractType, loadDate, fileName);
        String s3Key = "objects/" + fileName;
        PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        s3Client.putObject(putRequest, RequestBody.fromBytes(fileData));
        System.out.println("Uploaded to S3: " + s3Key);
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

    public static List<File> extractAllFromBinary(byte[] fileData, String fileName) {
        List<File> extractedFiles = new ArrayList<>();
        File tempDir;

        try {
            tempDir = Files.createTempDirectory("vault-extract").toFile();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temp directory", e);
        }

        try (
                ByteArrayInputStream byteStream = new ByteArrayInputStream(fileData);
                BufferedInputStream bufferedIn = new BufferedInputStream(byteStream);
                GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(bufferedIn);
                TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)
        ) {
            TarArchiveEntry entry;

            while ((entry = tarIn.getNextTarEntry()) != null) {
                if (entry.isDirectory()) {
                    continue;
                }

                File outFile = new File(tempDir, entry.getName());
                outFile.getParentFile().mkdirs();
                System.out.println("📁 Extracting: " + outFile.getAbsolutePath());

                try (OutputStream outStream = new BufferedOutputStream(new FileOutputStream(outFile))) {
                    IOUtils.copy(tarIn, outStream);
                }

                extractedFiles.add(outFile);
            }

        } catch (Exception e) {
            System.out.println("⚠️ Not a valid TAR archive. Falling back to plain GZIP: " + e.getMessage());

            try (
                    ByteArrayInputStream byteStream = new ByteArrayInputStream(fileData);
                    GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(byteStream)
            ) {
                File outFile = new File(tempDir, fileName.replace(".001", ""));
                try (OutputStream outStream = new BufferedOutputStream(new FileOutputStream(outFile))) {
                    IOUtils.copy(gzipIn, outStream);
                }
                extractedFiles.add(outFile);
            } catch (IOException ioException) {
                throw new RuntimeException("❌ Failed to decompress GZIP file", ioException);
            }
        }

        return extractedFiles;
    }

    public static File unzipGzipFile(byte[] compressedData, String outputFileName) {
        try {
            File tempDir = Files.createTempDirectory("unzipped-gzip").toFile();
            File outputFile = new File(tempDir, outputFileName.replace(".gz", ""));

            try (GZIPInputStream gzipIn = new GZIPInputStream(new ByteArrayInputStream(compressedData));
                 FileOutputStream outStream = new FileOutputStream(outputFile)) {
                IOUtils.copy(gzipIn, outStream);
            }

            System.out.println("✅ Unzipped file: " + outputFile.getAbsolutePath());
            return outputFile;

        } catch (IOException e) {
            throw new RuntimeException("❌ Failed to unzip GZIP file: " + outputFileName, e);
        }
    }

    public static List<File> extractAllToDirectory(byte[] fileData, File targetDir) {
        List<File> extractedFiles = new ArrayList<>();
        try (
                ByteArrayInputStream byteStream = new ByteArrayInputStream(fileData);
                BufferedInputStream bufferedIn = new BufferedInputStream(byteStream);
                GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(bufferedIn);
                TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)
        ) {
            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {
                if (entry.isDirectory()) continue;

                File outFile = new File(targetDir, entry.getName());
                outFile.getParentFile().mkdirs();

                try (OutputStream outStream = new BufferedOutputStream(new FileOutputStream(outFile))) {
                    IOUtils.copy(tarIn, outStream);
                }

                System.out.println("📁 Extracted: " + outFile.getAbsolutePath());
                extractedFiles.add(outFile);
            }
        } catch (IOException e) {
            throw new RuntimeException("❌ Failed to extract TAR.GZ properly", e);
        }

        return extractedFiles;
    }


}

