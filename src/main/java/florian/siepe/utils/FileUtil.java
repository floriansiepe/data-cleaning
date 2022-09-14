package florian.siepe.utils;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public enum FileUtil {
    ;

    public static List<File> findFiles(List<File> files) {
        return files.stream().flatMap(file -> {
            if (file.isDirectory()) {
                return Arrays.stream(Objects.requireNonNull(file.listFiles()))
                        .filter(File::isFile)
                        .filter(f -> f.getPath().endsWith(".csv"));
            } else {
                return Stream.of(file);
            }
        }).collect(Collectors.toList());
    }
}
