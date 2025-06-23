package com.veevaVault.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
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

    /*public byte[] downloadFile(String sessionId, String filePartName) throws IOException {
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
        String downloadFileName = filePartName.replace(".001", ".tar.gz");
        File desktopFile = new File("C:\\Users\\SUJEET\\Desktop\\veeva vault data", downloadFileName); // ✅ this will save only in veeva vault data

        if (desktopFile.exists()) {
            boolean deleted = desktopFile.delete();
            if (deleted) {
                System.out.println("🧹 Deleted extra Desktop copy: " + desktopFile.getAbsolutePath());
            }
        }

        // ✅ Step 2: Save .tar.gz to the target directory
        File localTarFile = new File("C:\\Users\\SUJEET\\Desktop\\veeva vault data", downloadFileName);
        Files.write(localTarFile.toPath(), fileData);
        System.out.println("✅ Saved to local: " + localTarFile.getAbsolutePath());

        // ✅ Step 3: Extract contents to a subfolder
        File extractDir = new File("C:\\Users\\SUJEET\\Desktop\\veeva vault data", filePartName.replace(".001", ""));
        if (!extractDir.exists()) extractDir.mkdirs();

        List<File> extractedFiles = S3StorageService.extractAllToDirectory(fileData, extractDir);

        // ✅ Step 4: Upload all extracted files to S3 preserving folder structure
        Files.walk(extractDir.toPath())
                .filter(Files::isRegularFile)
                .forEach(path -> {
                    try {
                        File file = path.toFile();
                        String relativePath = extractDir.toPath().relativize(path).toString().replace(File.separatorChar, '/');
                        byte[] extractedBytes = Files.readAllBytes(path);
                        System.out.println("Uploading to S3: " + relativePath);
                        s3StorageService.uploadToS3(extractedBytes, relativePath, extractType);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });


        // ✅ Step 5: Trigger Athena table update
        athenaService.createOrUpdateTable(extractType);

        return fileData;
    }*/
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
       // String downloadFileName = filePartName.replace(".001", ".tar.gz");
        String downloadFileName = filePartName.replaceAll("\\.\\d{3}$", ".tar.gz");


        File downloadDir = new File("C:\\Users\\SUJEET\\Desktop\\veeva vault data");
        if (!downloadDir.exists()) downloadDir.mkdirs();

        File localTarFile = new File(downloadDir, downloadFileName);
        if (localTarFile.exists()) {
            boolean deleted = localTarFile.delete();
            if (deleted) {
                System.out.println("🧹 Deleted old copy: " + localTarFile.getAbsolutePath());
            }
        }

        Files.write(localTarFile.toPath(), fileData);
        System.out.println("✅ Saved to local: " + localTarFile.getAbsolutePath());

        // ✅ Step 3: Extract contents to subfolder
        File extractDir = new File(downloadDir, filePartName.replace(".001", ""));
        if (!extractDir.exists()) extractDir.mkdirs();

        List<File> extractedFiles = S3StorageService.extractAllToDirectory(fileData, extractDir);

        // ✅ Step 4: Recursively collect all files for upload
        List<File> allExtractedFiles = new ArrayList<>();
        collectAllFilesRecursively(extractDir, allExtractedFiles);

        for (File extracted : allExtractedFiles) {
            String relativePath = extracted.getAbsolutePath()
                    .substring(extractDir.getAbsolutePath().length() + 1)
                    .replace(File.separatorChar, '/');

            System.out.println("Uploading to S3: " + relativePath);
            byte[] extractedBytes = Files.readAllBytes(extracted.toPath());
            s3StorageService.uploadToS3(extractedBytes, relativePath, extractType);
        }

        // ✅ Step 5: Trigger Athena table update
        athenaService.createOrUpdateTable(extractType);

        return fileData;
    }


    private String inferExtractType(String filePartName) {
        if (filePartName.contains("-F")) return "full_directdata";
        if (filePartName.contains("-N")) return "incremental_directdata";
        if (filePartName.contains("-L")) return "log_directdata";
        return "unknown";
    }

    private void collectAllFilesRecursively(File dir, List<File> fileList) {
        if (dir.isFile()) {
            fileList.add(dir);
        } else {
            File[] children = dir.listFiles();
            if (children != null) {
                for (File child : children) {
                    collectAllFilesRecursively(child, fileList);
                }
            }
        }
    }

}