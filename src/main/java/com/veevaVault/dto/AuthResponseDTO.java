package com.veevaVault.dto;
import lombok.Data;
import java.util.List;

@Data
public class AuthResponseDTO {
    private String responseStatus;
    private String sessionId;
    private int userId;
    private int vaultId;
    private List<VaultInfo> vaultIds;


}
