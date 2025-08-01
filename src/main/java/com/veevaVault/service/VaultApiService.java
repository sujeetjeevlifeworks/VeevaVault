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

    public void
    downloadFile(String sessionId, String filePartName) throws IOException {
        String url = BASE_URL + "/services/directdata/files/" + filePartName;
        byte[] fileData = downloadWithRetries(url, sessionId);
        System.out.println("Starting download...");
        System.out.println("Extracting archive...");
        System.out.println("Uploading to S3...");
        System.out.println("Done.");

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
        if (Files.exists(extractDir)) {
            deleteDirectoryRecursively(extractDir.toFile());
        }
        Files.createDirectories(extractDir);
        Files.createDirectories(extractDir);

        List<Path> extractedFiles = extractArchive(fileData, extractDir);
        uploadToS3(extractedFiles, extractDirName);
        //return fileData;
    }

    private void deleteDirectoryRecursively(File dir) {
        File[] allContents = dir.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectoryRecursively(file);
            }
        }
        dir.delete();
    }

    private List<Path> extractArchive(byte[] fileData, Path extractDir) throws IOException {
    List<Path> extractedFiles = new ArrayList<>();
    boolean extractionFailed = false;

    try (
            BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(fileData), 32768); // 32KB buffer
            GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(bis, true);
            TarArchiveInputStream tarIn = new TarArchiveInputStream(gzipIn)
    ) {
        TarArchiveEntry entry;
        while ((entry = tarIn.getNextTarEntry()) != null) {
            if (entry.isDirectory()) {
                File dir = new File(extractDir.toFile(), entry.getName());
                dir.mkdirs();
                continue;
            }


            Path normalizedPath = extractDir.resolve(entry.getName()).normalize();
            if (!normalizedPath.startsWith(extractDir)) {
                throw new IOException("Entry is outside target dir: " + entry.getName());
            }
            File outFile = normalizedPath.toFile();

            outFile.getParentFile().mkdirs();

            try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(outFile))) {
                byte[] buffer = new byte[8192];
                int len;
                while ((len = tarIn.read(buffer)) != -1) {
                    out.write(buffer, 0, len);
                }
            }

            // Decompress any internal .gz file
            if (outFile.getName().endsWith(".gz")) {
                File decompressed = new File(outFile.getAbsolutePath().replace(".gz", ""));
                try (GZIPInputStream gzipFileIn = new GZIPInputStream(new FileInputStream(outFile));
                     OutputStream fileOut = new FileOutputStream(decompressed)) {
                    gzipFileIn.transferTo(fileOut);
                }
                outFile.delete();
                extractedFiles.add(decompressed.toPath());
            } else {
                extractedFiles.add(outFile.toPath());
            }
        }
    } catch (Exception e) {
        extractionFailed = true;
    }

    checkKeyFiles(extractDir, extractedFiles);

    if (extractionFailed) {
        try {
            List<Path> recovered = recoverFiles(fileData, extractDir);
            for (Path p : recovered) {
                if (!extractedFiles.contains(p)) {
                    extractedFiles.add(p);
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
                if (!Files.exists(file)) continue;

                String fileName = file.getFileName().toString();
                String folderName = deriveFolderName(fileName);

                // Upload to S3
                try (InputStream input = Files.newInputStream(file)) {
                    long size = Files.size(file);
                    s3StorageService.uploadToS3(input, folderName + "/" + fileName, "objects", size);
                }

                // Create Athena table dynamically
                athenaService.createOrUpdateTable(folderName, fileName);

            } catch (IOException e) {
                System.out.println("Failed to upload or create Athena table for: " + file);
            }
        }
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
            retries--;
        }
        throw new IOException("Download failed");
    }

    private HttpHeaders createHeaders(String sessionId) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", sessionId);
        headers.setAccept(List.of());
        return headers;
    }
}
