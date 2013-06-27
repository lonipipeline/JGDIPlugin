/*
 Copyright 2000-2013  Laboratory of Neuro Imaging (LONI), <http://www.LONI.ucla.edu/>.

 This file is part of the LONI Pipeline Plug-ins (LPP), not the LONI Pipeline itself;
 see <http://pipeline.loni.ucla.edu/>.

 This plug-in program (not the LONI Pipeline) is free software: you can redistribute it
 and/or modify it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or  (at your option)
 any later version. The LONI Pipeline <http://pipeline.loni.ucla.edu/> has a different
 usage license <http://www.loni.ucla.edu/Policies/LONI_SoftwareAgreement.shtml>.

 This plug-in program is distributed in the hope that it will be useful, but WITHOUT ANY
 WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 A PARTICULAR PURPOSE.  See the GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>. 

 If you make improvements, modifications and extensions of the LONI Pipeline Plug-ins
 software,  you agree to share them with the LONI Pipeline developers and the broader   
 community according to the GPL license.
 */
package jgdiplugin.accounting;

import java.io.*;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.text.NumberFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Semaphore;
import java.util.zip.GZIPInputStream;
import plgrid.GridJobInfo;

/**
 * This class is responsible for maintaining its own database and store
 * information about recently finished jobs. SGE's default accounting files are
 * used as source. This is needed for Pipeline to retrieve finished job
 * information when it missed the finished job event.
 *
 * @author Petros Petrosyan
 */
public class SGEAccountingThread extends Thread {

    private String filepath;
    private final String JOB_ID_COLUMN = "JOB_ID";
    private final String START_TIME_COLUMN = "START_TIME";
    private final String END_TIME_COLUMN = "END_TIME";
    private final String EXIT_STATUS_COLUMN = "EXIT_STATUS";
    private final String FINISHED_JOBS_TABLE = "FINISHED_JOBS";
    private final String PARAMETERS_TABLE = "PARAMETERS";
    private final String LAST_SYNC_TIME_COLUMN = "LAST_SYNC_TIME";
    private final String LAST_SYNC_FILESIZE_COLUMN = "LAST_SYNC_FILESIZE";
    private static NumberFormat formatter = NumberFormat.getInstance();
    private boolean parsedRotatedFile;
    private boolean shutdown;
    private Semaphore semaphore;
    private final LinkedList<Connection> availableConnections;
    private final Set<Connection> allConnections;
    private static final int MAX_CONCURRENT_DB_CONNECTIONS = 4;
    private String databaseURL;
    private String dbUsername;
    private String dbPassword;

    public SGEAccountingThread(String sge_root, String sge_cell) {
        semaphore = new Semaphore(1, true);
        availableConnections = new LinkedList<Connection>();
        allConnections = new CopyOnWriteArraySet<Connection>();

        StringBuilder sb = new StringBuilder();

        sb.append(sge_root);
        sb.append(File.separator);
        sb.append(sge_cell);
        sb.append(File.separator);
        sb.append("common");
        sb.append(File.separator);
        sb.append("accounting");

        this.filepath = sb.toString();

        
        String database = new File("accountingDB").getAbsolutePath();

        databaseURL = "jdbc:hsqldb:" + database;
        dbUsername = "sa";
        dbPassword = "";

        Connection conn = null;

        try {

            Class.forName("org.hsqldb.jdbcDriver");
            conn = DriverManager.getConnection(databaseURL, dbUsername, dbPassword);



            Statement stmt = conn.createStatement();

            try {
                stmt.execute("CREATE TABLE " + FINISHED_JOBS_TABLE + " ( "
                        + JOB_ID_COLUMN + " VARCHAR(32),"
                        + START_TIME_COLUMN + " BIGINT,"
                        + END_TIME_COLUMN + " BIGINT,"
                        + EXIT_STATUS_COLUMN + " INT,"
                        + "PRIMARY KEY(" + JOB_ID_COLUMN + "))");
            } catch (Exception ex) {

                // Try to add primary key 
                try {
                    stmt.execute("ALTER TABLE " + FINISHED_JOBS_TABLE + " ADD PRIMARY KEY (" + JOB_ID_COLUMN + ")");
                } catch (Exception ex2) {
                    // HARMLESS
                }

            }


            try {
                stmt.execute("CREATE TABLE " + PARAMETERS_TABLE + " ("
                        + LAST_SYNC_TIME_COLUMN + " BIGINT,"
                        + LAST_SYNC_FILESIZE_COLUMN + " BIGINT )");

                stmt.execute("INSERT INTO " + PARAMETERS_TABLE + " ( "
                        + LAST_SYNC_TIME_COLUMN + ", " + LAST_SYNC_FILESIZE_COLUMN + ") VALUES (0,0)");


            } catch (Exception ex) {
                // HARMLESS 
            } finally {
                stmt.close();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        allConnections.add(conn);
        availableConnections.add(conn);

        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    // populate our cache of connections
                    for (int i = 1; i < MAX_CONCURRENT_DB_CONNECTIONS - 1; i++) {
                        Connection conn = DriverManager.getConnection(databaseURL, dbUsername, dbPassword);

                        availableConnections.add(conn);
                        allConnections.add(conn);
                        semaphore.release();
                    }
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            }
        };

        t.start();

    }

    private Connection acquireConnection() {
        Connection conn = null;
        try {
            semaphore.acquire();
        } catch (InterruptedException ex) {
            throw new IllegalArgumentException("Interrupted while trying to acquire a DB connection");
        }

        // we synchronize this next bit, so the LinkedList doesn't throw a
        // ConcurrentModificationException
        synchronized (SGEAccountingThread.class) {
            conn = availableConnections.removeFirst();
        }

        return conn;
    }

    private void releaseConnection(Connection conn) {

        // verify that the connection being returned to us is actually one of ours
        if (!allConnections.contains(conn)) {
            throw new IllegalArgumentException("You must release the connection given to you");
        }

        try {
            if (!conn.isValid(10)) {
                conn.close();
                // now we need to remove this connection from our set, and create a new one
                allConnections.remove(conn);
                availableConnections.remove(conn);

                try {
                    Class.forName("org.hsqldb.jdbcDriver");
                    conn = DriverManager.getConnection(databaseURL, dbUsername, dbPassword);
                    allConnections.add(conn);
                    availableConnections.add(conn);
                    // We do not add to available connections as this connections will be used now.
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        } catch (SQLException ex) {
        }

        synchronized (SGEAccountingThread.class) {
            availableConnections.add(conn);
        }

        semaphore.release();
    }

    private ResultSet executeQuery(String query) {
        Connection conn = acquireConnection();

        try {
            Statement stmt = conn.createStatement();

            stmt.setQueryTimeout(60);
            
            ResultSet ret = stmt.executeQuery(query);

            return ret;

        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            releaseConnection(conn);
        }

        return null;
    }

    private int executeUpdate(String query) {
        Connection conn = acquireConnection();
        try {
            Statement stmt = conn.createStatement();

            stmt.setQueryTimeout(60);
            return stmt.executeUpdate(query);

        } catch (SQLException ex) {
            ex.printStackTrace();
        } finally {
            releaseConnection(conn);
        }

        return -1;
    }

    private long getLastSyncTime() {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(LAST_SYNC_TIME_COLUMN);
        sb.append(" FROM ");
        sb.append(PARAMETERS_TABLE);

        try {

            ResultSet rs = executeQuery(sb.toString());
            if (rs != null && rs.next()) {
                return rs.getLong(LAST_SYNC_TIME_COLUMN);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return 0;
    }

    private long getLastSyncFileSize() {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(LAST_SYNC_FILESIZE_COLUMN);
        sb.append(" FROM ");
        sb.append(PARAMETERS_TABLE);

        try {

            ResultSet rs = executeQuery(sb.toString());
            if (rs != null && rs.next()) {
                return rs.getLong(LAST_SYNC_FILESIZE_COLUMN);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return 0;
    }

    private void updateLastSyncTime() {
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(PARAMETERS_TABLE);

        sb.append(" SET ");

        sb.append(LAST_SYNC_TIME_COLUMN);
        sb.append("=");
        sb.append(System.currentTimeMillis());

        executeUpdate(sb.toString());
    }

    private void updateLastSyncFileSize(long filesize) {
        StringBuilder sb = new StringBuilder("UPDATE ");
        sb.append(PARAMETERS_TABLE);

        sb.append(" SET ");

        sb.append(LAST_SYNC_FILESIZE_COLUMN);
        sb.append("=");
        sb.append(filesize);

        executeUpdate(sb.toString());
    }

    @Override
    public void run() {
        do {
            parseFile(filepath, true, true);
            
            if (shutdown) {
                for (Connection conn : allConnections) {
                    try {
                        conn.close();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
                break;
            }

            try {
                Thread.sleep(10000);
            } catch (Exception ex) {
                ex.printStackTrace();
            }

        } while (true);

    }

    /**
     * Copies a file from in to out using FileChannel
     */
    public static void copyFile(File in, File out)
            throws IOException {
        FileChannel inChannel = new FileInputStream(in).getChannel();
        FileChannel outChannel = new FileOutputStream(out).getChannel();
        try {
            inChannel.transferTo(0, inChannel.size(),
                    outChannel);
        } catch (IOException e) {
            throw e;
        } finally {
            if (inChannel != null) {
                inChannel.close();
            }
            if (outChannel != null) {
                outChannel.close();
            }
        }
    }

    public static String decompress(File file) throws Exception {
        GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(file));

        String outFilePath = file.getAbsolutePath().replace(".gz", "");
        OutputStream out = new FileOutputStream(outFilePath);

        byte[] buf = new byte[1024];
        int len;
        while ((len = gzipInputStream.read(buf)) > 0) {
            out.write(buf, 0, len);
        }

        gzipInputStream.close();
        out.close();

        file.delete();

        return outFilePath;
    }

    private String getDuration(long duration) {
        if (duration < 0) {
            //return "Negative Time Value"; 
            duration = 0;
        }

        formatter.setMinimumIntegerDigits(2);
        long weeks, days, hours, minutes, seconds, milliseconds;
        milliseconds = duration;
        duration /= 1000; // convert it to seconds

        seconds = duration % 60;
        minutes = (duration / 60) % 60;
        hours = (duration / 3600) % 24;
        days = (duration / 86400) % 7;
        weeks = duration / 604800;

        StringBuilder sb = new StringBuilder();
        if (weeks != 0) {
            sb.append(weeks);
            if (weeks == 1) {
                sb.append(" week, ");
            } else {
                sb.append(" weeks, ");
            }
        }
        if (days != 0) {
            sb.append(days);
            if (days == 1) {
                sb.append(" day, ");
            } else {
                sb.append(" days, ");
            }
        }

        if (hours != 0) {
            sb.append(hours);
            if (hours == 1) {
                sb.append(" hour, ");
            } else {
                sb.append(" hours, ");
            }
        }
        if (minutes != 0) {
            sb.append(minutes);
            if (minutes == 1) {
                sb.append(" minute, ");
            } else {
                sb.append(" minutes, ");
            }
        }

        // seconds get displayed no matter what
        sb.append(seconds);
        if (seconds == 1) {
            sb.append(" second");
        } else {
            sb.append(" seconds");
        }

        return sb.toString();
    }

    private void parseFile(String filePath, boolean updateDB, boolean continious) {
        System.out.println("SGE Accounting: File location " + filePath);

        boolean isZip = false;

        if (filePath.endsWith(".gz")) {
            File orig = new File(filePath);
            File copy = new File("./" + orig.getName());

            try {
                System.out.println("SGE Accounting: Copying " + orig.getPath() + " to " + copy.getPath());
                copyFile(orig, copy);
                System.out.println("SGE Accounting: Decompressing: " + copy.getAbsolutePath());
                filePath = decompress(copy);
                System.out.println("SGE Accounting: Decompressed: " + filePath);

            } catch (Exception ex) {
                ex.printStackTrace();
            }

            isZip = true;
        }


        File f = new File(filePath);

        if (updateDB) {
            long length = f.length();
            long lastSyncFileSize = getLastSyncFileSize();

            if (length < lastSyncFileSize || length == 0) {
                System.out.println("SGE Accounting: File has been rotated.");
                // File rotated, we need to open previous file and read all the entries from there
                // First check if we already parsed the rotated file for not repeating the process over n over
                if (!parsedRotatedFile) {
                    parseFile(filePath + ".0.gz", false, false);
                    updateLastSyncFileSize(0);
                }
                return;
            }
        }

        try {
            FileInputStream fis = new FileInputStream(f);

            final InputStreamReader isr = new InputStreamReader(fis);

            final BufferedReader br = new BufferedReader(isr);
            boolean log = false;
            long cutOffTime = 1000 * 60 * 60 * 24; // 24 hours
            long cleanupInterval = 1000 * 60 * 30; // 30 minutes
            long lastCleanupTime = 0;


            long lastSyncTime = getLastSyncTime();

            if (lastSyncTime > 0) {
                cutOffTime = System.currentTimeMillis() - lastSyncTime + 1000 * 60 * 5;
            }


            System.out.println("SGE Accounting: Cutoff time: " + getDuration(cutOffTime));

            int numJobsAdded = 0;

            while (true) {
                if (shutdown) {
                    break;
                }
                String response = br.readLine();
                
                if (response == null) {
                    if (!continious) {
                        break;
                    }

                    if (numJobsAdded > 0) {
                        if (updateDB) {
                            updateLastSyncTime();
                            updateLastSyncFileSize(f.length());
                        }
//                        System.out.println("SGE Accounting:" + numJobsAdded + " jobs added to db");
                        numJobsAdded = 0;
                    } else {
                        if (!f.exists() || f.length() < getLastSyncFileSize()) {
                            System.out.println("SGE Accounting: " + new Date() + ": File rotated.");
                            break;
                        }
                    }

                    if (System.currentTimeMillis() - lastCleanupTime > cleanupInterval) {
                        cleanup(cutOffTime);
                        lastCleanupTime = System.currentTimeMillis();
                    }

                    Thread.sleep(2000);
                    log = true;
                    continue;
                }

                FinishedJobRecord r = new FinishedJobRecord(response);

//                if (log) {
//
//                    System.out.println("SGE Accounting: Job " + r.job_number + "." + r.task_number
//                            + " finished ");// + "(qname=" + r.qname + ", endTime=" + r.end_time
////                            + " startTime=" + r.start_time + " exitStatus=" + r.exit_status + ")");
//                }

                
//                
//                
//                if (System.currentTimeMillis() - r.end_time < cutOffTime) {
//                    System.out.println("Persisting " + r.job_number);
//                    persist(r);
//                    numJobsAdded++;
//                } else { 
//                    System.out.println("NOT Persisting " + r.job_number);
//                }

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        if (isZip) {
            f.delete();
            parsedRotatedFile = true;

        }
    }

    public GridJobInfo getFinishedJobInfo(String jobId) {
        GridJobInfo gji = new GridJobInfo(jobId);
        gji.setState(GridJobInfo.STATE_NOT_FOUND);

        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(START_TIME_COLUMN);
        sb.append(",");
        sb.append(END_TIME_COLUMN);
        sb.append(",");
        sb.append(EXIT_STATUS_COLUMN);

        sb.append(" FROM ");
        sb.append(FINISHED_JOBS_TABLE);
        sb.append(" WHERE ");
        sb.append(JOB_ID_COLUMN);
        sb.append("='");
        sb.append(jobId);
        sb.append("'");


        try {

            ResultSet rs = executeQuery(sb.toString());
            if (rs != null && rs.next()) {
                long start_time = rs.getLong(START_TIME_COLUMN);
                long end_time = rs.getLong(END_TIME_COLUMN);
                int exit_status = rs.getInt(EXIT_STATUS_COLUMN);



                gji.setState(GridJobInfo.STATE_FINISHED);
                gji.setStartTime(start_time);
                gji.setFinishTime(end_time);
                gji.setExitStatus(exit_status);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return gji;
    }

    private int cleanup(long cutOffTime) {
        StringBuilder sb = new StringBuilder("DELETE FROM ");
        sb.append(FINISHED_JOBS_TABLE);
        sb.append(" WHERE ");
        sb.append(END_TIME_COLUMN);
        sb.append("<");
        sb.append(cutOffTime);

        int deleted = executeUpdate(sb.toString());

        if (deleted > 0) {
            System.out.println("SGE Accounting: Sweeper deleted " + deleted + " jobs from database.");
        }
        return deleted;
    }

    private void persist(FinishedJobRecord r) {
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(JOB_ID_COLUMN);
        sb.append(" FROM ");
        sb.append(FINISHED_JOBS_TABLE);
        sb.append(" WHERE ");
        sb.append(JOB_ID_COLUMN);
        sb.append("='");
        sb.append(r.job_number);

        if (r.task_number > 0) {
            sb.append(".");
            sb.append(r.task_number);
        }
        sb.append("'");

        try {
            ResultSet rs = executeQuery(sb.toString());
            if (rs != null && rs.next()) {;
                // already persisted
                return;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        sb = new StringBuilder("INSERT INTO ");

        sb.append(FINISHED_JOBS_TABLE);
        sb.append(" (");
        sb.append(JOB_ID_COLUMN);
        sb.append(",");
        sb.append(START_TIME_COLUMN);
        sb.append(",");
        sb.append(END_TIME_COLUMN);
        sb.append(",");
        sb.append(EXIT_STATUS_COLUMN);
        sb.append(") VALUES (?,?,?,?)");



        Connection conn = acquireConnection();

        try {
            PreparedStatement stmt = conn.prepareStatement(sb.toString());

            String jobId = r.job_number;

            if (r.task_number > 0) {
                jobId += "." + r.task_number;
            }

            long startTime = r.start_time;
            long endTime = r.end_time;

            if (startTime == endTime) {
                Double utime = Double.parseDouble(r.ru_utime);

                if (utime != null && utime > 0) {
                    endTime += utime * 1000;
                }
            }

            stmt.setString(1, jobId);
            stmt.setLong(2, r.start_time);
            stmt.setLong(3, r.end_time);
            stmt.setInt(4, r.exit_status);

            stmt.execute();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            releaseConnection(conn);
        }
    }

    public void shutdown() {
        shutdown = true;
    }
}
