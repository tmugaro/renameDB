package zw.org.mohcc.renamedb;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.File;
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


//        Path dir  = Paths.get("/home/onismo/Desktop/data");
        Path dir  = Paths.get("/home/administrator/backups/duplicate_db_handler/test_dir/input");

        Map<String, Integer> counters  = new HashMap<>();
        int[] counter  =  {0};

        Files.list(dir).forEach(path -> {
            if(path.toFile().isFile()){
                String content;
                try {
                    content = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }



                String s = "/*!40000 ALTER TABLE `laboratory_request_order` DISABLE KEYS */;\n";
                int index  = content.lastIndexOf(s);
                int nextIndex  = content.indexOf( "\n", index + s.length() +  1);


                if(index > -1 && nextIndex > -1){
                    String sql = content.substring(index + s.length() + 1, nextIndex);
                    String[] tokens = sql.split(",");
                    if(tokens.length > 0){
                        String name  = (tokens[5]+ "_"+tokens[6]).replace("'", "").replace(" ", "_");
                        counters.putIfAbsent(name, 0);
                        counters.put(name, counters.get(name) + 1);

                        name  = name + "_processed_"+ counters.get(name) + ".sql";

                        log.debug(name);

                        counter[0]++;


                        try {
//                            Files.write(new File("/home/onismo/Desktop/data/out/"+ name).toPath(), content.getBytes(StandardCharsets.UTF_8));
                            Files.write(new File("/home/administrator/backups/duplicate_db_handler/test_dir/output/"+ name).toPath(), content.getBytes(StandardCharsets.UTF_8));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                    }
                }
            }else{
                log.debug("Not a file");
            }


        });

        Set<String> keySet = counters.keySet();
        log.debug("Processed " + counter[0] + " sql files. However we have "+ keySet.size() + (keySet.size()==1 ? " database": "databases"));
    }
}
