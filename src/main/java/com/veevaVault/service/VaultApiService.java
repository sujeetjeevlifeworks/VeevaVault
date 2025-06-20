package com.veevaVault.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Service
public class VaultApiService {

    private final RestTemplate restTemplate;

    private final String BASE_URL = "https://partnersi-jeevlifeworks-safety.veevavault.com/api/v25.1";


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

    public byte[] downloadFile(String sessionId, String filePartName) {
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

        // Upload to S3
        s3StorageService.uploadToS3(fileData, filePartName, extractType);

        return fileData;
    }

    private String inferExtractType(String filePartName) {
        if (filePartName.contains("-F")) return "full_directdata";
        if (filePartName.contains("-N")) return "incremental_directdata";
        if (filePartName.contains("-L")) return "log_directdata";
        return "unknown";
    }
}