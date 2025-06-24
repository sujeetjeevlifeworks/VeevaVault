
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
    public List<Path> recoverFiles(byte[] fileData, Path extractDir) {
        List<Path> recoveredFiles = new ArrayList<>();
        File tempTar = null;

        try {
            // Save corrupted TAR.GZ to temp file
            tempTar = File.createTempFile("vault-recovery-", ".tar.gz");
            Files.write(tempTar.toPath(), fileData);

            try (
                    FileInputStream fis = new FileInputStream(tempTar);
                    BufferedInputStream bis = new BufferedInputStream(fis);
                    GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(bis);
                    TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)
            ) {
                TarArchiveEntry entry;
                while ((entry = tarIn.getNextTarEntry()) != null) {
                    if (!entry.isDirectory() && entry.getName().endsWith(".csv")) {
                        Path outputPath = extractDir.resolve(Paths.get(entry.getName()).getFileName().toString());
                        System.out.println("🔥 Recovered: " + outputPath.getFileName());
                        Files.createDirectories(outputPath.getParent());
                        try (OutputStream out = Files.newOutputStream(outputPath)) {
                            IOUtils.copy(tarIn, out);
                        }
                        recoveredFiles.add(outputPath);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("❌ Recovery failed: " + e.getMessage(), e);
        } finally {
            if (tempTar != null) {
                tempTar.delete();
            }
        }

        return recoveredFiles;
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

    public static List<File> extractAllToDirectory(byte[] fileData, File targetDir) {
        List<File> extractedFiles = new ArrayList<>();

        // Try normal extraction first
        try (TarArchiveInputStream tarIn = createTarStream(fileData)) {
            extractedFiles = tryExtractEntries(tarIn, targetDir);
            if (!extractedFiles.isEmpty()) return extractedFiles;
        } catch (Exception e) {
            log.warn("Normal extraction failed: {}", e.getMessage());
        }

        // If normal extraction failed, try streaming recovery
        try (InputStream input = new ByteArrayInputStream(fileData);
             GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(input);
             TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)) {

            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {
                try {
                    File extracted = extractEntry(entry, tarIn, targetDir);
                    if (extracted != null) {
                        extractedFiles.add(extracted);
                        log.info("Recovered file: {}", extracted);
                    }
                } catch (Exception e) {
                    log.error("Failed to recover entry {}: {}", entry.getName(), e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Streaming recovery failed: {}", e.getMessage());
        }

        return extractedFiles;
    }

    private static TarArchiveInputStream createTarStream(byte[] fileData) throws IOException {
        return new TarArchiveInputStream(
                new GzipCompressorInputStream(
                        new BufferedInputStream(
                                new ByteArrayInputStream(fileData))));
    }

    private static List<File> tryExtractEntries(TarArchiveInputStream tarIn, File targetDir) throws IOException {
        List<File> extractedFiles = new ArrayList<>();
        TarArchiveEntry entry;
        while ((entry = tarIn.getNextTarEntry()) != null) {
            File extracted = extractEntry(entry, tarIn, targetDir);
            if (extracted != null) {
                extractedFiles.add(extracted);
            }
        }
        return extractedFiles;
    }

    private static File extractEntry(TarArchiveEntry entry, TarArchiveInputStream tarIn, File targetDir)
            throws IOException {
        if (entry.isDirectory() || entry.getSize() <= 0) {
            return null;
        }

        String fileName = sanitizeFilename(entry.getName());
        File outFile = new File(targetDir, fileName);
        Files.copy(tarIn, outFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        return outFile;
    }
    private static List<File> tryExtractTarGz(byte[] fileData, File targetDir) {
        List<File> extractedFiles = new ArrayList<>();
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(fileData);
             BufferedInputStream bufferedIn = new BufferedInputStream(byteStream);
             GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(bufferedIn);
             TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)) {

            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {
                try {
                    File extracted = extractSingleFile(entry, tarIn, targetDir);
                    if (extracted != null) {
                        extractedFiles.add(extracted);
                    }
                } catch (Exception e) {
                    log.error("Failed to extract entry {}: {}", entry.getName(), e.getMessage());
                    // Continue with next file
                }
            }
        } catch (Exception e) {
            log.warn("TAR.GZ extraction failed: {}", e.getMessage());
        }
        return extractedFiles;
    }

    private static List<File> tryExtractPlainTar(byte[] fileData, File targetDir) {
        List<File> extractedFiles = new ArrayList<>();
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(fileData);
             TarArchiveInputStream tarIn = new TarArchiveInputStream(byteStream)) {

            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {
                try {
                    File extracted = extractSingleFile(entry, tarIn, targetDir);
                    if (extracted != null) {
                        extractedFiles.add(extracted);
                    }
                } catch (Exception e) {
                    log.error("Failed to extract entry {}: {}", entry.getName(), e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("Plain TAR extraction failed: {}", e.getMessage());
        }
        return extractedFiles;
    }

    private static List<File> trySalvageContent(byte[] fileData, File targetDir) {
        List<File> extractedFiles = new ArrayList<>();
        // Fallback to GZIP
        try (ByteArrayInputStream byteStream = new ByteArrayInputStream(fileData);
             GZIPInputStream gzipIn = new GZIPInputStream(byteStream)) {

            File outFile = new File(targetDir, "salvaged_content.bin");
            if (outFile.exists()) {
                Files.delete(outFile.toPath());
            }

            Files.copy(gzipIn, outFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            extractedFiles.add(outFile);
            log.info("Successfully salvaged content to: {}", outFile);
        } catch (Exception e) {
            log.error("GZIP salvage failed", e);
            throw new RuntimeException("Failed to extract archive using any method", e);
        }
        return extractedFiles;
    }

    private static File extractSingleFile(TarArchiveEntry entry, TarArchiveInputStream tarIn, File targetDir)
            throws IOException {
        // Skip directories and empty files
        if (entry.isDirectory() || entry.getSize() <= 0) {
            return null;
        }

        // Sanitize filename
        String fileName = sanitizeFilename(entry.getName());
        File outFile = new File(targetDir, fileName);

        // Ensure parent directories exist
        File parent = outFile.getParentFile();
        if (parent != null && !parent.exists()) {
            if (!parent.mkdirs()) {
                throw new IOException("Failed to create parent directories: " + parent);
            }
        }

        // Extract file
        try (OutputStream out = Files.newOutputStream(outFile.toPath())) {
            IOUtils.copy(tarIn, out);
            log.info("Successfully extracted: {}", outFile.getAbsolutePath());
            return outFile;
        }
    }

    private static String sanitizeFilename(String name) {
        // Remove path traversal attempts and other problematic characters
        return name.replaceAll("[\\\\/:*?\"<>|]", "_")
                .replaceAll("^\\.\\./", "")
                .replaceAll("^\\.\\.\\\\", "");
    }
}