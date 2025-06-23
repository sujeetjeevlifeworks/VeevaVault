package com.veevaVault.service;

import java.io.File;

public  class ExtractedFileInfo {
    public final File file;
    public final File tempRoot;

    public ExtractedFileInfo(File file, File tempRoot) {
        this.file = file;
        this.tempRoot = tempRoot;
    }
}
