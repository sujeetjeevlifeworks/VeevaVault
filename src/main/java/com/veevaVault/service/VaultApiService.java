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
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

@Service
public class VaultApiService {

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
        return fileName.replace(".csv", "").replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
    }

    public String getFiles(String sessionId, String extractType, String startTime, String stopTime) {
        StringBuilder urlBuilder = new StringBuilder(BASE_URL + "/services/directdata/files");
        urlBuilder.append("?extract_type=").append(extractType);

        if ("incremental_directdata".equalsIgnoreCase(extractType)) {
            if (startTime == null || stopTime == null) {
                throw new IllegalArgumentException("startTime and stopTime are required for incremental_directdata");
            }
            urlBuilder.append("&start_time=").append(startTime);
            urlBuilder.append("&stop_time=").append(stopTime);
        }

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", sessionId);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));

        HttpEntity<Void> entity = new HttpEntity<>(headers);
        return restTemplate.exchange(urlBuilder.toString(), HttpMethod.GET, entity, String.class).getBody();
    }

    public byte[] downloadFile(String sessionId, String filePartName) throws IOException {
        String url = BASE_URL + "/services/directdata/files/" + filePartName;
        byte[] fileData = downloadWithRetries(url, sessionId);

        if (!isValidArchive(fileData)) {
            throw new IOException("Invalid archive file");
        }

        String cleanFileName = filePartName.endsWith(".001")
                ? filePartName.replace(".001", ".tar.gz")
                : filePartName;

        Path archiveFile = BASE_DOWNLOAD_DIR.resolve(cleanFileName);
        Files.write(archiveFile, fileData);

        String extractDirName = cleanFileName.replace(".tar.gz", "");
        Path extractDir = BASE_DOWNLOAD_DIR.resolve(extractDirName);
        Files.createDirectories(extractDir);

        List<Path> extractedFiles = extractArchive(fileData, extractDir);
        uploadToS3(extractedFiles, extractDirName);
        return fileData;
    }



//new method is working for log and incrimental
    private List<Path> extractArchive(byte[] fileData, Path extractDir) throws IOException {
        List<Path> extractedFiles = new ArrayList<>();
        boolean extractionFailed = false;

        try (TarArchiveInputStream tarIn = new TarArchiveInputStream(
                new GzipCompressorInputStream(new ByteArrayInputStream(fileData)))) {

            TarArchiveEntry entry;
            while ((entry = tarIn.getNextTarEntry()) != null) {
                if (!entry.isDirectory()) {
                    String cleanName = getCleanFilename(entry.getName());
                    Path outputFile = extractDir.resolve(cleanName);
                    Files.createDirectories(outputFile.getParent());
                    Files.copy(tarIn, outputFile, StandardCopyOption.REPLACE_EXISTING);

                    // 🔽 Decompress .gz files if present inside tar
                    if (outputFile.toString().endsWith(".gz")) {
                        Path decompressed = Paths.get(outputFile.toString().replace(".gz", ""));
                        try (GZIPInputStream gzipIn = new GZIPInputStream(Files.newInputStream(outputFile));
                             OutputStream out = Files.newOutputStream(decompressed)) {
                            gzipIn.transferTo(out);
                        }
                        Files.delete(outputFile); // Optional: delete .gz
                        outputFile = decompressed;
                    }

                    extractedFiles.add(outputFile);
                }
            }
        } catch (Exception e) {
            extractionFailed = true;
        }

        checkKeyFiles(extractDir, extractedFiles);

        if (extractionFailed) {
            try {
                List<Path> recoveredFiles = recoverFiles(fileData, extractDir);
                for (Path recovered : recoveredFiles) {
                    if (!extractedFiles.contains(recovered)) {
                        extractedFiles.add(recovered);
                    }
                }
            } catch (Exception ignored) {}
        }

        return extractedFiles;
    }




    private void checkKeyFiles(Path extractDir, List<Path> extractedFiles) {
        Path manifestPath = extractDir.resolve("manifest.csv");
        Path metadataPath = extractDir.resolve("metadata_full.csv");

        if (Files.exists(manifestPath) && !extractedFiles.contains(manifestPath)) {
            extractedFiles.add(manifestPath);
        }
        if (Files.exists(metadataPath) && !extractedFiles.contains(metadataPath)) {
            extractedFiles.add(metadataPath);
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
                byte[] content = Files.readAllBytes(file);
                String folderName = deriveFolderName(fileName);
                s3StorageService.uploadToS3(content, folderName + "/" + fileName, "objects");
            } catch (IOException ignored) {}
        }

        athenaService.createOrUpdateTable(extractDirName);
    }

    private void uploadKeyFiles(List<Path> files, String extractDirName) {
        for (Path file : files) {
            try {
                String fileName = file.getFileName().toString();
                if (!fileName.equals("manifest.csv") && !fileName.equals("metadata_full.csv")) {
                    continue;
                }

                if (!Files.exists(file)) {
                    continue;
                }

                long size = Files.size(file);
                if (size > 0) {
                    byte[] content = Files.readAllBytes(file);
                    String folderName = deriveFolderName(fileName);
                    s3StorageService.uploadToS3(content, folderName + "/" + fileName, "objects");
                }
            } catch (IOException ignored) {}
        }
    }

    private List<Path> recoverFiles(byte[] fileData, Path extractDir) throws IOException {
        List<Path> recoveredFiles = new ArrayList<>();

        try (GZIPInputStream gzipIn = new GZIPInputStream(new ByteArrayInputStream(fileData));
             BufferedReader reader = new BufferedReader(new InputStreamReader(gzipIn))) {

            String line;
            String currentFileName = null;
            BufferedWriter writer = null;

            while ((line = reader.readLine()) != null) {
                if (line.trim().endsWith(".csv")) {
                    if (writer != null) {
                        writer.close();
                    }
                    currentFileName = getCleanFilename(line.trim());
                    Path outputFile = extractDir.resolve(currentFileName);
                    Files.createDirectories(outputFile.getParent());
                    writer = Files.newBufferedWriter(outputFile);
                    recoveredFiles.add(outputFile);
                } else if (writer != null) {
                    writer.write(line);
                    writer.newLine();
                }
            }

            if (writer != null) {
                writer.close();
            }
        } catch (Exception e) {
            throw e;
        }

        return recoveredFiles;
    }

    private String getCleanFilename(String originalName) {
        if (originalName.contains("manifest.csv")) return "manifest.csv";
        if (originalName.contains("metadata_full.csv")) return "metadata_full.csv";

        String filename = originalName.substring(originalName.lastIndexOf('/') + 1)
                .substring(originalName.lastIndexOf('\\') + 1);

        filename = filename.replaceAll("[\\\\/:*?\"<>|]", "_")
                .replaceAll("^[\\s.]+", "").trim();

        if (!filename.toLowerCase().endsWith(".csv") && filename.contains(".")) {
            filename = filename.substring(0, filename.lastIndexOf('.')) + ".csv";
        } else if (!filename.toLowerCase().endsWith(".csv")) {
            filename = filename + ".csv";
        }

        return filename;
    }

    private boolean isValidArchive(byte[] data) {
        return data != null && data.length > 2 && data[0] == 0x1F && data[1] == (byte) 0x8B;
    }

    private byte[] downloadWithRetries(String url, String sessionId) throws IOException {
        int retries = 3;

        while (retries > 0) {
            try {
                ResponseEntity<byte[]> response = restTemplate.exchange(
                        url,
                        HttpMethod.GET,
                        new HttpEntity<>(createHeaders(sessionId)),
                        byte[].class
                );
                byte[] data = response.getBody();
                if (data != null && data.length > 1024) {
                    return data;
                }
            } catch (Exception e) {
                retries--;
                if (retries == 0) {
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
