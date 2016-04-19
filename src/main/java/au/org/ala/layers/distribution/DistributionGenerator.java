/**************************************************************************
 * Copyright (C) 2010 Atlas of Living Australia
 * All Rights Reserved.
 * <p>
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 * <p>
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 ***************************************************************************/

package au.org.ala.layers.distribution;

import au.org.ala.layers.util.SpatialUtil;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Adam
 */
public class DistributionGenerator {

    private static final Logger logger = Logger.getLogger(DistributionGenerator.class);

    static int CONCURRENT_THREADS = 6;
    static String db_url = "jdbc:postgresql://localhost:5432/layersdb";
    static String db_usr = "postgres";
    static String db_pwd = "postgres";

    private static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("org.postgresql.Driver");
            String url = db_url;
            conn = DriverManager.getConnection(url, db_usr, db_pwd);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        return conn;
    }

    static public void main(String[] args) {
        logger.info("Calculates and fills empty area_km in table distributionshapes.\n\nargs[0] = threadcount, args[1] = db connection string,\n args[2] = db username,\n args[3] = password\n");
        if (args.length >= 4) {
            CONCURRENT_THREADS = Integer.parseInt(args[0]);
            db_url = args[1];
            db_usr = args[2];
            db_pwd = args[3];
        }
        long start = System.currentTimeMillis();
        while (updateArea() > 0) {
            logger.info("time since start= " + (System.currentTimeMillis() - start) + "ms");
        }
    }

    private static int updateArea() {
        Connection conn = null;
        try {
            conn = getConnection();
            String sql = "SELECT id, ST_AsText(the_geom) as wkt FROM distributionshapes WHERE area_km is null"
                    + " limit 100";
            if (conn == null) {
                logger.error("connection is null");
            } else {
                logger.debug("connection is not null");
            }
            Statement s1 = conn.createStatement();
            ResultSet rs1 = s1.executeQuery(sql);

            LinkedBlockingQueue<String[]> data = new LinkedBlockingQueue<String[]>();
            while (rs1.next()) {
                data.put(new String[]{rs1.getString("id"), rs1.getString("wkt")});
            }

            logger.info("next " + data.size());

            int size = data.size();

            if (size == 0) {
                return 0;
            }

            CountDownLatch cdl = new CountDownLatch(data.size());

            AreaThread[] threads = new AreaThread[CONCURRENT_THREADS];
            for (int j = 0; j < CONCURRENT_THREADS; j++) {
                threads[j] = new AreaThread(data, cdl, getConnection().createStatement());
                threads[j].start();
            }

            cdl.await();

            for (int j = 0; j < CONCURRENT_THREADS; j++) {
                try {
                    threads[j].s.getConnection().close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                threads[j].interrupt();
            }
            return size;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
        return 0;
    }
}

class AreaThread extends Thread {

    private static final Logger logger = Logger.getLogger(AreaThread.class);

    Statement s;
    LinkedBlockingQueue<String[]> lbq;
    CountDownLatch cdl;

    public AreaThread(LinkedBlockingQueue<String[]> lbq, CountDownLatch cdl, Statement s) {
        this.s = s;
        this.cdl = cdl;
        this.lbq = lbq;
    }

    @Override
    public void run() {
        try {
            while (true) {

                try {
                    String[] data = lbq.take();

                    double area = SpatialUtil.calculateArea(data[1]) / 1000.0 / 1000.0;

                    String sql = "UPDATE distributionshapes SET area_km = " + area + " WHERE id='" + data[0] + "';";

                    int update = s.executeUpdate(sql);
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                cdl.countDown();
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
