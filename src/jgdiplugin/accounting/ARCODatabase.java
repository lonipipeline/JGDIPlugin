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

import java.sql.*;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Semaphore;
import plgrid.FinishedJobInfo;
import plgrid.PipelineGridPlugin;

/**
 *
 * @author Petros Petrosyan
 */
public class ARCODatabase {

    private Semaphore semaphore;
    private final LinkedList<Connection> availableConnections;
    private final Set<Connection> allConnections;
    private String arcoURL;
    private String arcoUsername;
    private String arcoPassword;
    private boolean useSGEArrayJobs;
    private PipelineGridPlugin plugin;
    private boolean initialized;

    public ARCODatabase(PipelineGridPlugin plugin) {
        this.plugin = plugin;

        final int numConnections = 5;
        semaphore = new Semaphore(numConnections, true);
        allConnections = new CopyOnWriteArraySet<Connection>();
        availableConnections = new LinkedList<Connection>();

        Map<String, String> preferences = plugin.getPreferences();

        arcoURL = preferences.get("GridJobAccountingURL");
        arcoUsername = preferences.get("GridJobAccountingUsername");
        arcoPassword = preferences.get("GridJobAccountingPassword");
        useSGEArrayJobs = Boolean.parseBoolean(preferences.get("GridUseArrayJobs"));

        if (arcoURL != null && arcoURL.length() > 0) {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                // populate our cache of connections
                for (int i = 0; i < numConnections; i++) {
                    Connection conn = DriverManager.getConnection(arcoURL, arcoUsername, arcoPassword);
                    availableConnections.add(conn);
                }
                initialized = true;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        allConnections.addAll(availableConnections);
    }

    public void shutdown() {
        for (Connection conn : allConnections) {
            try {
                conn.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        allConnections.clear();
        availableConnections.clear();
    }

    public Connection acquireConnection() {
        try {
            semaphore.acquire();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        // we synchronize this next bit, so the LinkedList doesn't throw a
        // ConcurrentModificationException
        synchronized (ARCODatabase.class) {
            return availableConnections.removeFirst();
        }
    }

    public void reconnectConnection(Connection connection) {

        // verify that the connection being returned to us is actually one of ours
        if (!allConnections.contains(connection)) {
            throw new IllegalArgumentException("You must reconnect the connection given to you");
        }

        synchronized (ARCODatabase.class) {

            allConnections.remove(connection);

            do {
                try {
                    System.out.println(" Reconnecting to ARCO database");
                    Class.forName("com.mysql.jdbc.Driver");
                    connection = DriverManager.getConnection(arcoURL, arcoUsername, arcoPassword);
                    allConnections.add(connection);
                    availableConnections.add(connection);
                    System.out.println(" ARCO database Reconnection success !");
                    break;
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

                // if connection fails, we will try again to reconnect until it is successfull
                // because this connection is very important to Pipeline.
                try {
                    Thread.sleep(3000);
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

            } while (true);
        }
        semaphore.release();
    }

    public void releaseConnection(Connection connection) {

        // verify that the connection being returned to us is actually one of ours
        if (!allConnections.contains(connection)) {
            throw new IllegalArgumentException("You must release the connection given to you");
        }

        synchronized (ARCODatabase.class) {
            try {
                if (connection.isClosed()) {
                    System.out.println("Connection to ARCO database is closed. Creating new connection.");    // added an exception to get the stack trace

                    // now we need to remove this connection from our set, and create a new one
                    allConnections.remove(connection);

                    try {
                        Class.forName("com.mysql.jdbc.Driver");
                        connection = DriverManager.getConnection(arcoURL, arcoUsername, arcoPassword);
                        allConnections.add(connection);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        return;
                    }
                }
            } catch (SQLException sqlEx) {
                sqlEx.printStackTrace();

                reconnectConnection(connection);
            }
            availableConnections.add(connection);
        }
        semaphore.release();
    }

    public FinishedJobInfo getFinishedJobInfo(String jobId) {
        if (!initialized) {
            return null;
        }

        FinishedJobInfo fji = new FinishedJobInfo(jobId);

        Connection connection = null;
        Statement statement = null;
        try {
            ResultSet rs = null;

            do {
                connection = acquireConnection();
                statement = connection.createStatement();

                String query = "SELECT ju_end_time, ju_exit_status, ju_start_time "
                        + "FROM sge_job, sge_job_usage "
                        + "WHERE j_id=ju_parent "
                        + "AND j_job_number=";

                if (useSGEArrayJobs && jobId.contains(".")) {
                    String j_id = jobId.substring(0, jobId.indexOf("."));
                    String t_id = jobId.substring(jobId.indexOf(".") + 1);

                    query += j_id + " AND j_task_number=" + t_id;
                } else {
                    query += jobId;
                }

                query += " ORDER BY ju_end_time DESC";

                try {
                    rs = statement.executeQuery(query);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    reconnectConnection(connection);
                }

                if (rs == null) {
                    statement.close();
                }

            } while (rs == null);


            if (rs.next()) {
                do {

                    Timestamp end_timestamp = rs.getTimestamp("ju_end_time");
                    Timestamp start_timestamp = rs.getTimestamp("ju_start_time");
                    int exit_status = rs.getInt("ju_exit_status");

                    long end_time = end_timestamp.getTime();
                    long start_time = start_timestamp.getTime();

                    // If found real end time for the job, break the loop
                    if (end_time > 0 && start_time > 0) {
                        fji.setExitStatus(exit_status);
                        fji.setStartTimestamp(start_timestamp);
                        fji.setEndTimestamp(end_timestamp);

                        return fji;
                    }
                } while (rs.next()); // There can be multiple items for same job id

            } else {
                return null;
            }

            rs.close();
            statement.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (connection != null) {
                releaseConnection(connection);
            }
        }

        return fji;
    }
}
