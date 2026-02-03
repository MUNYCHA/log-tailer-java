package org.munycha.logtailer.producer;

import org.munycha.logtailer.model.MountPathStorageUsage;

import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class MountPathStorageUsageCollector {


    private static final String HOST_FS =
            System.getenv().getOrDefault("HOST_FS", "");

    public static List<MountPathStorageUsage> collect(List<String> paths) {
        List<MountPathStorageUsage> result = new ArrayList<>();

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

                MountPathStorageUsage ps = new MountPathStorageUsage();
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


