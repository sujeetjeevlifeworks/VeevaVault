package com.veevaVault.service;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

@Service
public class VaultApiService {
    private static final String CLASS_NAME = "VaultApiService";

    private final RestTemplate restTemplate;
    private final String BASE_URL = "https://partnersi-jeevlifeworks-safety.veevavault.com/api/v25.1";
    private final Path BASE_DOWNLOAD_DIR = Paths.get("C:\\Users\\SUJEET\\Desktop\\veeva vault data");

    @Autowired
    private AthenaService athenaService;

    @Autowired
    private S3StorageService s3StorageService;

    @Autowired
    public VaultApiService(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    public String authenticate(String username, String password) {

        String authUrl = BASE_URL + "/auth";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));

        MultiValueMap<String, String> form = new LinkedMultiValueMap<>();
        form.add("username", username);
        form.add("password", password);

        HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(form, headers);

        try {
            ResponseEntity<String> response = restTemplate.postForEntity(authUrl, request, String.class);
            return response.getBody();
        } catch (Exception e) {
            throw new RuntimeException("Authentication failed!", e);
        }
    }
    private String deriveFolderName(String fileName) {
        return fileName.replace(".csv", "")
                .replaceAll("[^a-zA-Z0-9]", "")
                .toLowerCase();
    }


    public String getFiles(String sessionId, String extractType, String startTime, String stopTime) {
        String methodName = "getFiles";
        System.out.printf("[%s.%s] Getting files for extractType: %s%n", CLASS_NAME, methodName, extractType);

        StringBuilder urlBuilder = new StringBuilder(BASE_URL + "/services/directdata/files");
        urlBuilder.append("?extract_type=").append(extractType);

        if ("incremental_directdata".equalsIgnoreCase(extractType)) {
            if (startTime == null || stopTime == null) {
                throw new IllegalArgumentException("startTime and stopTime are required for incremental_directdata");
            }
            urlBuilder.append("&start_time=").append(startTime);
            urlBuilder.append("&stop_time=").append(stopTime);
        }

        String url = urlBuilder.toString();
        System.out.printf("[%s.%s] Request URL: %s%n", CLASS_NAME, methodName, url);

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", sessionId);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));

        HttpEntity<Void> entity = new HttpEntity<>(headers);
        return restTemplate.exchange(url, HttpMethod.GET, entity, String.class).getBody();
    }

    public byte[] downloadFile(String sessionId, String filePartName) throws IOException {
        String methodName = "downloadFile";
        System.out.printf("[%s.%s] Downloading file: %s%n", CLASS_NAME, methodName, filePartName);

        String url = BASE_URL + "/services/directdata/files/" + filePartName;
        byte[] fileData = downloadWithRetries(url, sessionId);

        if (!isValidArchive(fileData)) {
            System.err.printf("[%s.%s] ❌ Invalid archive file%n", CLASS_NAME, methodName);
            throw new IOException("Invalid archive file");
        }

        // Handle .001 extension by renaming to .tar.gz
        String cleanFileName = filePartName;
        if (filePartName.endsWith(".001")) {
            cleanFileName = filePartName.replace(".001", ".tar.gz");
        }

        // Save original archive to base directory with correct extension
        Path archiveFile = BASE_DOWNLOAD_DIR.resolve(cleanFileName);
        Files.write(archiveFile, fileData);
        System.out.printf("[%s.%s] 💾 Saved original archive: %s%n", CLASS_NAME, methodName, archiveFile);

        // Create extraction directory
        String extractDirName = cleanFileName.replace(".tar.gz", "");
        Path extractDir = BASE_DOWNLOAD_DIR.resolve(extractDirName);
        Files.createDirectories(extractDir);
        System.out.printf("[%s.%s] Created extraction directory: %s%n", CLASS_NAME, methodName, extractDir);

        // Extract files
        List<Path> extractedFiles = extractArchive(fileData, extractDir);

        // Upload to S3 with proper structure
        uploadToS3(extractedFiles, extractDirName);

        return fileData;
    }

    private List<Path> extractArchive(byte[] fileData, Path extractDir) throws IOException {
        String methodName = "extractArchive";
        System.out.printf("[%s.%s] Extracting archive to: %s%n", CLASS_NAME, methodName, extractDir);

        List<Path> extractedFiles = new ArrayList<>();
        boolean extractionFailed = false;

        // First try normal extraction
        try (TarArchiveInputStream tarIn = new TarArchiveInputStream(
                new GzipCompressorInputStream(new ByteArrayInputStream(fileData)))) {

            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {
                if (!entry.isDirectory()) {
                    String originalName = entry.getName();
                    System.out.printf("[%s.%s] Found file in archive: %s%n", CLASS_NAME, methodName, originalName);
                    String cleanName = getCleanFilename(originalName);
                    Path outputFile = extractDir.resolve(cleanName);

                    Files.createDirectories(outputFile.getParent());
                    Files.copy(tarIn, outputFile, StandardCopyOption.REPLACE_EXISTING);
                    extractedFiles.add(outputFile);
                    System.out.printf("[%s.%s] ✅ Extracted: %s%n", CLASS_NAME, methodName, outputFile.getFileName());
                }
            }
        } catch (Exception e) {
            System.err.printf("[%s.%s] ⚠️ Partial extraction failed: %s%n", CLASS_NAME, methodName, e.getMessage());
            extractionFailed = true;
        }

        // Verify key files were extracted
        checkKeyFiles(extractDir, extractedFiles);

        // If extraction failed, try to recover remaining files
        if (extractionFailed) {
            try {
                System.out.printf("[%s.%s] Attempting file recovery...%n", CLASS_NAME, methodName);
                List<Path> recoveredFiles = recoverFiles(fileData, extractDir);

                // Add only files that weren't already extracted
                for (Path recovered : recoveredFiles) {
                    if (!extractedFiles.contains(recovered)) {
                        extractedFiles.add(recovered);
                        System.out.printf("[%s.%s] 🔥 Recovered: %s%n", CLASS_NAME, methodName, recovered.getFileName());
                    }
                }
            } catch (Exception e) {
                System.err.printf("[%s.%s] ⚠️ Recovery failed: %s%n", CLASS_NAME, methodName, e.getMessage());
            }
        }

        return extractedFiles;
    }

    private void checkKeyFiles(Path extractDir, List<Path> extractedFiles) {
        String methodName = "checkKeyFiles";
        Path manifestPath = extractDir.resolve("manifest.csv");
        Path metadataPath = extractDir.resolve("metadata_full.csv");

        try {
            // Check manifest.csv
            if (Files.exists(manifestPath)) {
                System.out.printf("[%s.%s] ✔ Manifest file exists: %s (size: %d bytes)%n",
                        CLASS_NAME, methodName, manifestPath, Files.size(manifestPath));
                if (!extractedFiles.contains(manifestPath)) {
                    extractedFiles.add(manifestPath);
                }
            } else {
            }

            // Check metadata_full.csv
            if (Files.exists(metadataPath)) {
                System.out.printf("[%s.%s] ✔ Metadata file exists: %s (size: %d bytes)%n",
                        CLASS_NAME, methodName, metadataPath, Files.size(metadataPath));
                if (!extractedFiles.contains(metadataPath)) {
                    extractedFiles.add(metadataPath);
                }
            } else {
            }
        } catch (IOException e) {
        }
    }

    private void uploadToS3(List<Path> files, String extractDirName) throws IOException {
        uploadKeyFiles(files, extractDirName);

        for (Path file : files) {
            try {
                if (!Files.exists(file)) {
                    continue;
                }

                String fileName = file.getFileName().toString();
                long size = Files.size(file);


                byte[] content = Files.readAllBytes(file);
              //  String folderName = deriveFolderName(fileName);
                String folderName = deriveFolderName(fileName);
               // s3StorageService.uploadToS3(content, fileName, "objects/" + folderName);
                s3StorageService.uploadToS3(content, folderName + "/" + fileName, "objects");


            } catch (IOException e) {

            }
        }

        // Trigger Athena table creation/update

        athenaService.createOrUpdateTable(extractDirName);
    }

    private void uploadKeyFiles(List<Path> files, String extractDirName) {
        String methodName = "uploadKeyFiles";
        System.out.printf("[%s.%s] Checking key files for upload%n", CLASS_NAME, methodName);

        for (Path file : files) {
            try {
                String fileName = file.getFileName().toString();
                if (!fileName.equals("manifest.csv") && !fileName.equals("metadata_full.csv")) {
                    continue;
                }

                if (!Files.exists(file)) {
                    System.err.printf("[%s.%s] ❌ Key file missing: %s%n", CLASS_NAME, methodName, fileName);
                    continue;
                }

                long size = Files.size(file);
                System.out.printf("[%s.%s] 🔍 Processing key file: %s (Size: %d bytes)%n",
                        CLASS_NAME, methodName, fileName, size);

                if (size > 0) {
                    byte[] content = Files.readAllBytes(file);


                    String folderName = deriveFolderName(fileName);
                    s3StorageService.uploadToS3(content, folderName + "/" + fileName, "objects");




                    System.out.printf("[%s.%s] 📤 Uploaded key file to S3: %s%n", CLASS_NAME, methodName, fileName);
                } else {
                    System.err.printf("[%s.%s] ⚠️ Empty key file: %s%n", CLASS_NAME, methodName, fileName);
                }
            } catch (IOException e) {
                System.err.printf("[%s.%s] ❌ Failed to upload key file %s: %s%n",
                        CLASS_NAME, methodName, file.getFileName(), e.getMessage());
            }
        }
    }

    private List<Path> recoverFiles(byte[] fileData, Path extractDir) throws IOException {
        String methodName = "recoverFiles";
        System.out.printf("[%s.%s] Attempting file recovery%n", CLASS_NAME, methodName);

        List<Path> recoveredFiles = new ArrayList<>();

        try (GZIPInputStream gzipIn = new GZIPInputStream(new ByteArrayInputStream(fileData));
             BufferedReader reader = new BufferedReader(new InputStreamReader(gzipIn))) {

            String line;
            String currentFileName = null;
            BufferedWriter writer = null;

            while ((line = reader.readLine()) != null) {
                if (line.trim().endsWith(".csv")) {
                    // Close previous file if open
                    if (writer != null) {
                        writer.close();
                    }

                    // Create valid filename
                    currentFileName = getCleanFilename(line.trim());
                    Path outputFile = extractDir.resolve(currentFileName);

                    // Ensure parent directories exist
                    Files.createDirectories(outputFile.getParent());

                    writer = Files.newBufferedWriter(outputFile);
                    recoveredFiles.add(outputFile);
                    System.out.printf("[%s.%s] 🔥 Recovered: %s%n", CLASS_NAME, methodName, outputFile.getFileName());
                } else if (writer != null) {
                    writer.write(line);
                    writer.newLine();
                }
            }

            if (writer != null) {
                writer.close();
            }
        } catch (Exception e) {
            System.err.printf("[%s.%s] ❌ Recovery failed: %s%n", CLASS_NAME, methodName, e.getMessage());
            throw e;
        }

        return recoveredFiles;
    }

    private String getCleanFilename(String originalName) {
        // Handle manifest and metadata files specially
        if (originalName.contains("manifest.csv")) {
            return "manifest.csv";
        }
        if (originalName.contains("metadata_full.csv")) {
            return "metadata_full.csv";
        }

        // Original logic for other files
        String filename = originalName.substring(originalName.lastIndexOf('/') + 1)
                .substring(originalName.lastIndexOf('\\') + 1);

        filename = filename.replaceAll("[\\\\/:*?\"<>|]", "_")
                .replaceAll("^[\\s.]+", "")
                .trim();

        if (!filename.toLowerCase().endsWith(".csv") && filename.contains(".")) {
            filename = filename.substring(0, filename.lastIndexOf('.')) + ".csv";
        } else if (!filename.toLowerCase().endsWith(".csv")) {
            filename = filename + ".csv";
        }

        return filename;
    }

    private boolean isValidArchive(byte[] data) {
        // GZIP magic number check (0x1f 0x8b)
        return data != null && data.length > 2 && data[0] == 0x1F && data[1] == (byte)0x8B;
    }

    private byte[] downloadWithRetries(String url, String sessionId) throws IOException {
        String methodName = "downloadWithRetries";
        int retries = 3;
        int attempt = 1;

        while (retries > 0) {
            System.out.printf("[%s.%s] Download attempt %d/%d%n", CLASS_NAME, methodName, attempt, 3);
            try {
                ResponseEntity<byte[]> response = restTemplate.exchange(
                        url,
                        HttpMethod.GET,
                        new HttpEntity<>(createHeaders(sessionId)),
                        byte[].class
                );
                byte[] data = response.getBody();
                if (data != null && data.length > 1024) {
                    System.out.printf("[%s.%s] ✅ Download successful (%d bytes)%n",
                            CLASS_NAME, methodName, data.length);
                    return data;
                }
            } catch (Exception e) {
                retries--;
                attempt++;
                if (retries == 0) {
                    System.err.printf("[%s.%s] ❌ Download failed after retries: %s%n",
                            CLASS_NAME, methodName, e.getMessage());
                    throw new IOException("Download failed after retries", e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Download interrupted", ie);
                }
            }
        }
        throw new IOException("Download failed");
    }

    private HttpHeaders createHeaders(String sessionId) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", sessionId);
        headers.setAccept(List.of());
        return headers;
    }

    private String inferExtractType(String filePartName) {
        if (filePartName.contains("-F")) return "full_directdata";
        if (filePartName.contains("-N")) return "incremental_directdata";
        if (filePartName.contains("-L")) return "log_directdata";
        return "unknown";
    }
}