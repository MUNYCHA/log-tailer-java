package org.munycha.logTailer.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;

public class ConfigLoader {

    private final String filePath;

    public ConfigLoader(String filePath) {
        this.filePath = filePath;
    }

    public AppConfig load() throws Exception {

        try (InputStream inputStream = loadConfigFile(filePath)) {

            if (inputStream == null) {
                throw new FileNotFoundException(
                        "Config file not found (external or internal): " + filePath
                );
            }

            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(inputStream, AppConfig.class);
        }
    }

    private InputStream loadConfigFile(String filePath) throws FileNotFoundException {

        File externalFile = new File(filePath);

        if (externalFile.exists()) {
            System.out.println(
                    "[ConfigLoader] Loading EXTERNAL config: " + externalFile.getAbsolutePath()
            );
            return new FileInputStream(externalFile);
        }

        System.out.println(
                "[ConfigLoader] External config not found. Loading INTERNAL config: " + filePath
        );
        return getClass().getClassLoader().getResourceAsStream(filePath);
    }
}
