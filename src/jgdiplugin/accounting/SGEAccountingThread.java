/*
 Copyright 2000-2012  Laboratory of Neuro Imaging (LONI), <http://www.LONI.ucla.edu/>.

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
    private final Connection dbConnection;
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

    public SGEAccountingThread(String sge_root, String sge_cell) {
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

        Connection conn = null;


        try {

            Class.forName("org.hsqldb.jdbcDriver");
            conn = DriverManager.getConnection("jdbc:hsqldb:" + database, "sa", "");



            Statement stmt = conn.createStatement();

            try {
                stmt.execute("CREATE TABLE " + FINISHED_JOBS_TABLE + " ( "
                        + JOB_ID_COLUMN + " VARCHAR(32),"
                        + START_TIME_COLUMN + " BIGINT,"
                        + END_TIME_COLUMN + " BIGINT,"
                        + EXIT_STATUS_COLUMN + " INT )");
            } catch (Exception ex) {
                // HARMLESS 
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

        dbConnection = conn;

    }

    private ResultSet executeQuery(String query) {

//        System.out.println("executeQuery:" + query);

        synchronized (dbConnection) {
            try {
                Statement stmt = dbConnection.createStatement();

                return stmt.executeQuery(query);

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return null;
    }

    private int executeUpdate(String query) {

        synchronized (dbConnection) {
            try {
                Statement stmt = dbConnection.createStatement();

                return stmt.executeUpdate(query);

            } catch (Exception ex) {
                ex.printStackTrace();
            }
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

        if (dbConnection == null) {
            System.err.println("Error: Failed to initialize connection to database. SGE Accounting thread is not able to start.");
            return;
        }

        int i = 1;
        do {
            parseFile(filepath, true, true);
            i++;

            if (shutdown) {
                try {
                    dbConnection.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
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

                if (System.currentTimeMillis() - r.end_time < cutOffTime) {
                    persist(r);
                    numJobsAdded++;
                }

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
        if (dbConnection == null) {
            System.err.println("SGEAccountingThread: The database connection cannot be null.");
            return null;
        }

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
        if (dbConnection == null) {
            return 0;
        }

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
        if (dbConnection == null) {
            return;
        }

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
            if (rs != null && rs.next()) {
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


        synchronized (dbConnection) {
            try {
                PreparedStatement stmt = dbConnection.prepareStatement(sb.toString());

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
            }
        }
    }

    public void shutdown() {
        shutdown = true;
    }
}
