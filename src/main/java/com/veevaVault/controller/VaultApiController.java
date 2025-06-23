package com.veevaVault.controller;

import com.veevaVault.service.VaultApiService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/api/vault")
public class VaultApiController {

    @Autowired
    private VaultApiService vaultApiService;

    @PostMapping("/authenticate")
    public ResponseEntity<String> authenticate(@RequestParam String username, @RequestParam String password) {
        String sessionId = vaultApiService.authenticate(username, password);
        return ResponseEntity.ok(sessionId);
    }

    @GetMapping("/files")
    public ResponseEntity<String> getFiles(@RequestParam String sessionId,
                                           @RequestParam String extractType,
                                           @RequestParam(required = false) String startTime,
                                           @RequestParam(required = false) String stopTime) {
        String response = vaultApiService.getFiles(sessionId, extractType, startTime, stopTime);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/download")
    public ResponseEntity<byte[]> downloadFile(@RequestParam String sessionId, @RequestParam String fileName) throws IOException {
        byte[] data = vaultApiService.downloadFile(sessionId, fileName);

        // Convert file part name like 56006-20250617-0000-F.001 → 56006-20250617-0000-F.tar.gz
        String downloadFileName = fileName.replace(".001", ".tar.gz");

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        headers.setContentDisposition(ContentDisposition.attachment().filename(downloadFileName).build());

        return new ResponseEntity<>(data, headers, HttpStatus.OK);
    }
}
