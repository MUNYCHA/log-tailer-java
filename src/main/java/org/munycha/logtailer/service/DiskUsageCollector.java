package org.munycha.logtailer.service;

import org.munycha.logtailer.model.DiskUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class DiskUsageCollector {

    private static final Logger log = LoggerFactory.getLogger(DiskUsageCollector.class);

    private static final String HOST_FS =
            System.getenv().getOrDefault("HOST_FS", "");

    public static List<DiskUsage> collect(List<String> paths) {
        List<DiskUsage> result = new ArrayList<>();

        for (String p : paths) {
            try {
                Path realPath = resolvePath(p);
                if (!Files.exists(realPath)) {
                    log.warn("Storage path does not exist, skipping | path={}", realPath);
                    continue;
                }

                FileStore store = Files.getFileStore(realPath);

                long total = store.getTotalSpace();
                long free  = store.getUnallocatedSpace();   // total free bytes including OS-reserved blocks
                long used  = total - free;
                double usedPercent = total > 0
                        ? (double) used * 100.0 / total
                        : 0.0;

                DiskUsage ps = new DiskUsage();
                ps.setPath(p);
                ps.setTotalBytes(total);
                ps.setUsedBytes(used);
                ps.setUsedPercent(usedPercent);

                result.add(ps);

            } catch (Exception e) {
                log.warn("Failed to collect disk usage | path={}", p, e);
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


