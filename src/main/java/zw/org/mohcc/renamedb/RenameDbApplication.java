package zw.org.mohcc.renamedb;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

@Slf4j
@SpringBootApplication
public class RenameDbApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(RenameDbApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {


//        Path dir = Paths.get("/home/onismo/Desktop/data");
//        Path dir  = Paths.get("/home/administrator/backups/duplicate_db_handler/test_dir/input");

        Map<String, Integer> counters = new HashMap<>();
        int[] counter = {0};
//        File folder = new File("/home/onismo/Desktop/data");
//        File folder = new File("/home/administrator/backups/duplicate_db_handler/test_dir/input");
        File folder = new File("/data/duplicate_db_handler/test_dir/input");
        File[] listOfFiles = folder.listFiles();
        String s = "/*!40000 ALTER TABLE `laboratory_request_order` DISABLE KEYS */;";
        assert listOfFiles != null;
        for (File file : listOfFiles) {
            if (file.isFile()) {

                try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        // process
                        if (line.contains(s) && line.startsWith(s)) {
                            String sqlLine = br.readLine();
                            String[] tokens = sqlLine.split(",");
                            if (tokens.length > 5) {
                                String name = (tokens[5] + "_" + tokens[6]).replace("'", "").replace(" ", "_");
                                counters.putIfAbsent(name, 0);
                                counters.put(name, counters.get(name) + 1);
                                name = name + "_processed_" + counters.get(name) + ".sql";
                                Files.move(file.toPath(), file.toPath().resolveSibling("out/"+name));
                                log.debug(name);

                                counter[0]++;

                            }

                            break;
                        }

                    }
                }


            } else {
                log.debug("not file");
            }
        }

        Set<String> keySet = counters.keySet();
        log.debug("Processed " + counter[0] + " sql files. However we have " + keySet.size() + (keySet.size() == 1 ? " database" : "databases"));
    }
}
