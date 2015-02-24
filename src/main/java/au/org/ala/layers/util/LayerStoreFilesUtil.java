package au.org.ala.layers.util;

import au.org.ala.layers.dto.StoreRequest;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Created by a on 28/10/14.
 */
public class LayerStoreFilesUtil {

    /**
     * Log4j instance
     */
    private static final String[] VALID_DIRECTORIES = {"diva", "diva_cache", "shape", "shape_diva", "analysis", "tabulation"};
    private static final Logger LOGGER = Logger.getLogger(LayerStoreFilesUtil.class);
    private static Thread thread = null;
    private static Map log = new ConcurrentSkipListMap();

    /**
     * Get layer files.
     * <p/>
     * Returns requested contents of layers/ready zipped.
     * <p/>
     * Inputs
     * <p/>
     * filter list containing one or more of;
     * - diva_cache
     * - diva
     * - shape
     * - shape_diva
     * - analysis
     * <p/>
     * apiKey is required for auth
     * <p/>
     * optional exclude (filesnames with timestamps)
     * <p/>
     * optional include (filenames)
     * <p/>
     * <p/>
     * Use
     * <p/>
     * - Retrieve all layer files
     * {
     * apiKey: ...,
     * filter: ["diva", "shape", "shape_diva"],
     * }
     * <p/>
     * - Retrieve a specific diva file (include file prefix)
     * {
     * apiKey: ...,
     * filter: ["diva"],
     * include: ["diva/clay30"]
     * }
     * <p/>
     * - Sync layers files (excludes are files that are only required if they have been updated)
     * {
     * apiKey: ...,
     * filter: ["diva", "shape", "shape_diva"],
     * exclude: [ { filename: "diva/clay30.grd", lastModified: (as Long) } ]
     * }
     *
     * @return zip stream
     */
    public static void writeFilesZippedToStream(String layerFilesPath, OutputStream stream, StoreRequest request) throws Exception {

        try {
            BufferedOutputStream bos = new BufferedOutputStream(stream);
            ZipOutputStream zos = new ZipOutputStream(bos);

            //zip dummy incase output is empty
            ZipEntry ze = new ZipEntry("blank");
            zos.putNextEntry(ze);
            zos.write(1);
            zos.closeEntry();


            String[] dirs = VALID_DIRECTORIES;
            if (request.getFilter() != null) {
                dirs = new String[request.getFilter().size()];
                request.getFilter().toArray(dirs);
            }
            for (String dir : dirs) {
                if (ArrayUtils.contains(VALID_DIRECTORIES, dir)) {
                    for (File f : new File(layerFilesPath + dir).listFiles()) {
                        //if it is a directory, enter first level
                        if (f.isDirectory()) {
                            for (File sub : f.listFiles()) {
                                zipFile(sub, dir + "/" + f.getName(), request, zos);
                            }
                        } else {
                            zipFile(f, dir, request, zos);
                        }
                    }
                }
            }

            zos.close();
        } catch (Exception e) {
            LOGGER.error("failed to return layers files.", e);
            throw new RuntimeException("failed to get files");
        }
    }

    private static void zipFile(File f, String dir, StoreRequest request, ZipOutputStream zos) throws IOException {
        if (!f.isFile()) {
            return;
        }

        String fname = dir + "/" + f.getName();
        boolean include = true;

        //check exclude
        if (request.getExclude() != null) {
            for (int i = 0; i < request.getExclude().size() && include; i++) {
                include = !request.getExclude().get(i).get("filename").equals(fname)
                        || Long.parseLong(String.valueOf(request.getExclude().get(i).get("lastModified"))) < f.lastModified();
            }
        }
        //check include
        if (request.getInclude() != null) {
            include = false;
            for (int i = 0; i < request.getInclude().size() && !include; i++) {
                include = request.getInclude().get(i).equals(fname) || fname.startsWith(request.getInclude().get(i) + ".");
            }
        }

        if (include) {
            ZipEntry ze = new ZipEntry(fname);
            zos.putNextEntry(ze);

            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f));
            byte[] b = new byte[1024 * 1024];
            int n;
            while ((n = bis.read(b)) > 0) {
                zos.write(b, 0, n);
            }

            bis.close();
            zos.closeEntry();
        }
    }

    /**
     * Sync by fetching files from another layers-service
     * <p/>
     * <p/>
     * Use
     * <p/>
     * - Sync layer files
     * {
     * apiKey: ...,
     * layersServiceUrl: ...,
     * filter: ["diva", "shape", "shape_diva"]
     * }
     *
     * @throws Exception
     */
    public synchronized static Map sync(String layerFilesPath, StoreRequest request, boolean waitUntilFinished) throws Exception {

        final String apiKeyFinal = request.getApiKey();
        final String layersServiceUrlFinal = request.getLayersServiceUrl();
        final List filterFinal = request.getFilter();
        final List includeFinal = request.getInclude();
        final String layerFilesPathFinal = layerFilesPath;

        if (thread == null || !thread.isAlive() || waitUntilFinished) {

            Thread t = new Thread() {
                @Override
                public void run() {

                    try {
                        //fetch and unzip stream
                        HttpClient client = new HttpClient();
                        PostMethod post = new PostMethod(layersServiceUrlFinal + "/store/get");

                        post.addRequestHeader("Content-Type", "application/json");

                        JSONObject jo = new JSONObject();
                        jo.put("apiKey", apiKeyFinal);
                        jo.put("filter", filterFinal);

                        if (includeFinal != null) {
                            jo.put("include", includeFinal);
                        }

                        //get existing files to build excludes
                        JSONArray exclude = new JSONArray();
                        for (Object o : filterFinal) {
                            String dir = (String) o;
                            if (ArrayUtils.contains(VALID_DIRECTORIES, dir)) {
                                for (File f : new File(layerFilesPathFinal + "/" + dir).listFiles()) {
                                    JSONObject fo = new JSONObject();
                                    fo.put("filename", dir + "/" + f.getName());
                                    fo.put("lastModified", f.lastModified());
                                    exclude.add(fo);
                                }
                            }
                        }
                        jo.put("exclude", exclude);

                        post.setRequestEntity(new StringRequestEntity(jo.toString(), "text/xml", "UTF-8"));

                        int result = client.executeMethod(post);

                        ZipInputStream zis = new ZipInputStream(new BufferedInputStream(post.getResponseBodyAsStream()));

                        ZipEntry ze;
                        String pth = layerFilesPathFinal + "/forSync/";
                        File filePth = new File(pth);
                        try {
                            FileUtils.deleteDirectory(filePth);
                        } catch (Exception e) {
                            //OK if it does not exist
                        }
                        FileUtils.forceMkdir(filePth);

                        while ((ze = zis.getNextEntry()) != null) {
                            FileUtils.forceMkdir(new File(new File(pth + ze.getName()).getParent()));
                            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(pth + ze.getName()));
                            byte[] b = new byte[1024];
                            int n;
                            while ((n = zis.read(b)) > 0) {
                                bos.write(b, 0, n);
                            }
                            bos.close();
                            long len = new File(pth + ze.getName()).length();
                            int typeIdx = 0;
                            String[] types = {"B", "KB", "MB"};
                            while (len / 1024 > 0 && typeIdx < types.length - 1) {
                                typeIdx++;
                                len = len / 1024;
                            }
                            String size = String.valueOf(len) + " " + types[typeIdx];
                            log.put(new SimpleDateFormat("yyyy/MM/dd hh:mm:ss.SSS").format(new Date()), "Downloaded " + ze.getName() + " (" + size + ")");
                        }

                        zis.close();

                        //copy new files into the correct directories
                        for (File src : filePth.listFiles()) {
                            if (src.isDirectory()) {
                                File dst = new File(layerFilesPathFinal + "/" + src.getName());

                                String size = String.valueOf(FileUtils.sizeOfDirectory(src) / 1024 / 1024) + " MB";

                                FileUtils.copyDirectory(src, dst);
                                FileUtils.deleteDirectory(src);

                                log.put(new SimpleDateFormat("yyyy/MM/dd hh:mm:ss.SSS").format(new Date()), "Copied " + src.getName() + " (" + size + ")");
                            }
                        }

                        //re-cache
                        log.put(new SimpleDateFormat("yyyy/MM/dd hh:mm:ss.SSS").format(new Date()), "FINISHED");
                    } catch (Exception e) {
                        log.put(new SimpleDateFormat("yyyy/MM/dd hh:mm:ss.SSS").format(new Date()), "ERROR " + e);
                        LOGGER.error("error in store/pullRequest", e);

                        throw new RuntimeException("failed to send");
                    }
                }
            };

            log.put(new SimpleDateFormat("yyyy/MM/dd hh:mm:ss.SSS").format(new Date()), "STARTED: " + layersServiceUrlFinal + " filter=" + filterFinal);

            if (!waitUntilFinished) {
                thread = t;
                thread.start();
            } else {
                t.run();
            }
        }

        return log;
    }

    public static Map getLog() {
        return log;
    }
}
