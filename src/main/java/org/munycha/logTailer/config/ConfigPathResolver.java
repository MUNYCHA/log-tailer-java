package org.munycha.logTailer.config;

public final class ConfigPathResolver {

    private static final String ENV_KEY = "LOGTAILER_CONFIG";
    private static final String SYS_PROP_KEY = "logtailer.config";
    private static final String DEFAULT_PATH = "config/logTailer_config.json";
    private static final String ARG_PREFIX = "--config";

    private ConfigPathResolver() {
    }

    public static String resolve(String[] args) {

        // 1. CLI arguments (highest priority)
        String cliPath = resolveFromArgs(args);
        if (cliPath != null) {
            return cliPath;
        }

        // 2. Environment variable
        String envPath = System.getenv(ENV_KEY);
        if (envPath != null && !envPath.isBlank()) {
            return envPath;
        }

        // 3. JVM system property
        String sysPropPath = System.getProperty(SYS_PROP_KEY);
        if (sysPropPath != null && !sysPropPath.isBlank()) {
            return sysPropPath;
        }

        // 4. Default
        return DEFAULT_PATH;
    }

    private static String resolveFromArgs(String[] args) {

        if (args == null) {
            return null;
        }

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            // --config=/path/to/file
            if (arg.startsWith(ARG_PREFIX + "=")) {
                return arg.substring((ARG_PREFIX + "=").length());
            }

            // --config /path/to/file
            if (arg.equals(ARG_PREFIX) && i + 1 < args.length) {
                return args[i + 1];
            }

            // positional argument (fallback)
            if (!arg.startsWith("-") && !arg.isBlank()) {
                return arg;
            }
        }

        return null;
    }
}
