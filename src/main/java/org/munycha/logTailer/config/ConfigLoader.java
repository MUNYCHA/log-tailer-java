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

        // Prefer external file if it exists and is a regular file
        if (externalFile.isFile()) {
            return new FileInputStream(externalFile);
        }

        // Fallback to classpath resource
        InputStream internalStream =
                getClass().getClassLoader().getResourceAsStream(filePath);

        if (internalStream != null) {
            System.out.println(
                    "[ConfigLoader] External config not found, using INTERNAL config: " + filePath
            );
            return internalStream;
        }

        // Neither external nor internal exists
        return null;
    }

}
