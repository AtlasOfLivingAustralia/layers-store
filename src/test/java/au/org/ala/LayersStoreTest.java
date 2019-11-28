package au.org.ala;

import au.org.ala.layers.client.Client;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.io.*;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class LayersStoreTest {
    public JdbcTemplate jdbcTemplate;

    @Before
    public void setup() {
        System.setProperty("layers.store.config.path", "./src/test/resources/layers-store-config.properties");

        String tmpDir = System.getProperty("java.io.tmpdir");
        System.out.println("tmpdir: " + tmpDir);

        System.setProperty("LAYER_INDEX_URL", "");

        System.setProperty("ALASPATIAL_OUTPUT_PATH", tmpDir);
        System.setProperty("ALASPATIAL_OUTPUT_PATH", tmpDir);
        System.setProperty("ANALYSIS_LAYER_FILES_PATH", tmpDir);
        System.setProperty("LAYER_FILES_PATH", tmpDir);


        try {
            // copy test data dir
            File srcTestDataDir = new File("./src/test/resources/test-data");
            File testDataDir = new File(tmpDir + "/test-data");
            FileUtils.copyDirectory(srcTestDataDir, testDataDir);

            // unzip test data
            for (File file : testDataDir.listFiles()) {
                if (file.getName().endsWith(".zip")) {
                    unzip(file.getPath(), testDataDir.getPath());
                }
            }

            // data source
            DataSource dataSource = (DataSource) Client.getContext().getBean("dataSource");
            jdbcTemplate = new JdbcTemplate(dataSource);

            Client.getLayerIntersectDao().getConfig().load();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void unzip(String zipFile, String outputDir) throws IOException {
        ZipFile zip = new ZipFile(zipFile);
        Enumeration entries = zip.entries();

        while (entries.hasMoreElements()) {
            ZipEntry entry = (ZipEntry) entries.nextElement();
            byte[] buffer = new byte[1024];
            InputStream zipin = zip.getInputStream(entry);
            BufferedOutputStream fileout = new BufferedOutputStream(new FileOutputStream(outputDir + "/" + entry.getName()));

            int len;
            while ((len = zipin.read(buffer)) >= 0) {
                fileout.write(buffer, 0, len);
            }

            zipin.close();
            fileout.flush();
            fileout.close();
        }
    }
}
