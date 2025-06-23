package com.veevaVault.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Service
public class VaultApiService {

    private final RestTemplate restTemplate;

    private final String BASE_URL = "https://partnersi-jeevlifeworks-safety.veevavault.com/api/v25.1";


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
            System.out.println("✅ Vault Auth Response: " + response.getBody());
            return response.getBody();
        } catch (Exception e) {
            throw new RuntimeException("Authentication failed!", e);
        }
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

        String url = urlBuilder.toString();

        HttpHeaders headers = new HttpHeaders();
        headers.set("Authorization", sessionId);
        headers.setAccept(List.of(MediaType.APPLICATION_JSON));

        HttpEntity<Void> entity = new HttpEntity<>(headers);
        return restTemplate.exchange(url, HttpMethod.GET, entity, String.class).getBody();
    }

   public byte[] downloadFile(String sessionId, String filePartName) throws IOException {
       String url = BASE_URL + "/services/directdata/files/" + filePartName;

       HttpHeaders headers = new HttpHeaders();
       headers.set("Authorization", sessionId);
       headers.setAccept(List.of());

       HttpEntity<Void> entity = new HttpEntity<>(headers);

       ResponseEntity<byte[]> response = restTemplate.exchange(
               url,
               HttpMethod.GET,
               entity,
               byte[].class
       );

       byte[] fileData = response.getBody();
       String extractType = inferExtractType(filePartName);

       // ✅ Step 1: Upload original .001 archive to S3 (renamed as .tar.gz)
       String archiveFileName = filePartName.replace(".001", ".tar.gz");
       s3StorageService.uploadToS3(fileData, archiveFileName, extractType);

       // ✅ Step 2: Extract all files with full folder structure
       List<File> extractedFiles = S3StorageService.extractAllFromBinary(fileData, filePartName);

       // ✅ Step 3: Upload each extracted file to S3
       for (File extracted : extractedFiles) {
           // Determine relative path within the temp dir, preserving TAR structure
           File tempDir = extracted.getParentFile().getParentFile(); // parent of 'Object', 'Metadata', etc.
           String relativePath = extracted.getAbsolutePath()
                   .substring(tempDir.getAbsolutePath().length() + 1)
                   .replace(File.separatorChar, '/'); // Normalize for S3

           byte[] extractedBytes = java.nio.file.Files.readAllBytes(extracted.toPath());
           s3StorageService.uploadToS3(extractedBytes, relativePath, extractType);

       }

       athenaService.createOrUpdateTable(extractType);

       return fileData;
   }

    private String inferExtractType(String filePartName) {
        if (filePartName.contains("-F")) return "full_directdata";
        if (filePartName.contains("-N")) return "incremental_directdata";
        if (filePartName.contains("-L")) return "log_directdata";
        return "unknown";
    }
}