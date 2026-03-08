package org.munycha.logtailer.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class ConfigLoader {

    private static final Logger log = LoggerFactory.getLogger(ConfigLoader.class);

    private static final ObjectMapper mapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final String filePath;

    public ConfigLoader(String filePath) {
        this.filePath = filePath;
    }

    public AppConfig load() throws IOException {
        try (InputStream inputStream = loadConfigFile(filePath)) {
            AppConfig config = mapper.readValue(inputStream, AppConfig.class);
            config.validate();
            return config;
        }
    }

    private InputStream loadConfigFile(String filePath) throws FileNotFoundException {

        File externalFile = new File(filePath);

        if (externalFile.isFile()) {
            return new FileInputStream(externalFile);
        }

        InputStream internalStream =
                getClass().getClassLoader().getResourceAsStream(filePath);

        if (internalStream != null) {
            log.warn("External config not found, using internal classpath config: {}", filePath);
            return internalStream;
        }

        throw new FileNotFoundException(
                "Config file not found (external or classpath): " + filePath
        );
    }
}

