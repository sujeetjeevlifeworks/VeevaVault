package com.veevaVault.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Data
@Component
public class VaultConfig {

    @Value("${vault.username}")
    private String username;

    @Value("${vault.password}")
    private String password;

    @Value("${vault.url}")
    private String baseUrl;

    private String sessionId;
    private long sessionCreatedAt;

    public boolean isSessionExpired() {
        return (System.currentTimeMillis() - sessionCreatedAt) > 600_000; // 10 minutes
    }
}

