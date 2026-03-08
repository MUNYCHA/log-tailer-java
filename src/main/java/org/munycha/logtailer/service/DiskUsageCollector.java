package org.munycha.logtailer.service;

import org.munycha.logtailer.model.DiskUsage;

import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class DiskUsageCollector {


    private static final String HOST_FS =
            System.getenv().getOrDefault("HOST_FS", "");

    public static List<DiskUsage> collect(List<String> paths) {
        List<DiskUsage> result = new ArrayList<>();

        for (String p : paths) {
            try {
                Path realPath = resolvePath(p);
                if (!Files.exists(realPath)) {
                    continue;
                }

                FileStore store = Files.getFileStore(realPath);

                long total = store.getTotalSpace();
                long usable = store.getUsableSpace();
                long used = total - usable;
                double usedPercent = total > 0
                        ? (double) used * 100.0 / total
                        : 0.0;

                DiskUsage ps = new DiskUsage();
                ps.setPath(p);
                ps.setTotalBytes(total);
                ps.setUsedBytes(used);
                ps.setUsedPercent(usedPercent);

                result.add(ps);

            } catch (Exception ignored) {
            }
        }
        return result;
    }

    private static Path resolvePath(String path) {
        if (HOST_FS.isEmpty()) {
            return Paths.get(path);
        }
        return Paths.get(HOST_FS + path);
    }
}


