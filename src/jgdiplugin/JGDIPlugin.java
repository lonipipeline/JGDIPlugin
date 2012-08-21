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
package jgdiplugin;

import com.sun.grid.jgdi.EventClient;
import com.sun.grid.jgdi.JGDI;
import com.sun.grid.jgdi.JGDIException;
import com.sun.grid.jgdi.JGDIFactory;
import com.sun.grid.jgdi.configuration.*;
import com.sun.grid.jgdi.event.EventTypeEnum;
import java.util.*;
import jgdiplugin.accounting.ARCODatabase;
import jgdiplugin.accounting.SGEAccountingThread;
import plgrid.FinishedJobInfo;
import plgrid.GridJobInfo;
import plgrid.GridJobSubmitInfo;
import plgrid.PipelineGridPlugin;
import plgrid.exception.PLGrid_InvalidMethodException;

/**
 * This is a LONI Pipeline's Grid Plugin class. It provides communication with
 * grid manager.
 *
 * This plugin is designed to work with Sun Grid Engine.
 *
 * @author Petros Petrosyan
 */
public class JGDIPlugin extends PipelineGridPlugin {

    public JGDIPlugin() {

        // SGE Environment variables are required to be set before creating this object.
        SGE_ROOT = System.getenv("SGE_ROOT");
        SGE_CELL = System.getenv("SGE_CELL");

        // SGE_PORT is not defined by SGE by default, it is not required if default port is used.
        String sge_port = System.getenv("SGE_PORT");

        if (sge_port == null) {
            SGE_PORT = "6444"; // use default port if env. variable is not set
        } else {
            SGE_PORT = sge_port;
        }

        System.out.println("SGE Root: " + SGE_ROOT);
        System.out.println("SGE Cell: " + SGE_CELL);
        System.out.println("SGE Port: " + SGE_PORT);

        // i.e bootstrap:///usr/sge@loni:6444
        bootstrapURL = "bootstrap://" + SGE_ROOT + "@" + SGE_CELL + ":" + SGE_PORT;

        try {
            jgdi = JGDIFactory.newSynchronizedInstance(bootstrapURL);
            registerListeners();
        } catch (Exception ex) {
            ex.printStackTrace();
            return;
        }

        HeartBeatTimerTask tt = new HeartBeatTimerTask();

        // Create a timer which will do a heart beat check of SGE's Qmaster
        heartBeatTimer = new Timer();
        heartBeatTimer.scheduleAtFixedRate(tt, PING_QMASTER_INTERVAL_MS, PING_QMASTER_INTERVAL_MS);

        System.out.println("JGDIPlugin (version: " + JGDI_PLUGIN_VERSION + ") started.");
    }

    private class HeartBeatTimerTask extends TimerTask {

        @Override
        public void run() {
            pingQmaster();
        }
    }

    @Override
    public void setPreferences(Map<String, String> prefs) {
        super.setPreferences(prefs);

        finishedJobRetrievalMethod = prefs.get("GridFinishedJobRetrievalMethod");

        if (finishedJobRetrievalMethod == null || finishedJobRetrievalMethod.trim().length() == 0) {

            if (arcoDatabase != null) {
                // turn off ARCo if it is on.
                arcoDatabase = new ARCODatabase(this);
                arcoDatabase.shutdown();
            }

            if (sgeAccountingThread == null) {
                // turn on SGE Accounting thread if it is off.
                sgeAccountingThread = new SGEAccountingThread(SGE_ROOT, SGE_CELL);
                sgeAccountingThread.start();
            }
        } else if (finishedJobRetrievalMethod.toLowerCase().equals("arco")) {
            if (sgeAccountingThread != null) {
                // turn off SGE Accounting thread if it is on.
                sgeAccountingThread.shutdown();
            }

            if (arcoDatabase == null) {
                // turn on SGE ARCo if it is off.
                arcoDatabase = new ARCODatabase(this);
            }
        }

    }

    private void registerListeners() throws JGDIException {
        jobFinishListener = new JGDIJobFinishListener(this);
        finishEventClient = JGDIFactory.createEventClient(bootstrapURL, 0);
        finishEventClient.subscribe(EventTypeEnum.JobFinalUsage);
        finishEventClient.subscribe(EventTypeEnum.JobDel);
        finishEventClient.commit();
        finishEventClient.addEventListener(jobFinishListener);

        jobModListener = new JGDIJobModListener(this);
        modEventClient = JGDIFactory.createEventClient(bootstrapURL, 0);
        modEventClient.subscribe(EventTypeEnum.JobTaskMod);
        modEventClient.commit();
        modEventClient.addEventListener(jobModListener);

        qmasterDownListener = new JGDIQmasterDownListener(this);
        downEventClient = JGDIFactory.createEventClient(bootstrapURL, 0);
        downEventClient.subscribe(EventTypeEnum.QmasterGoesDown);
        downEventClient.commit();
        downEventClient.addEventListener(qmasterDownListener);
    }

    private boolean isQmasterAlive() {
        return isQmasterAlive;
    }

    public void pingQmaster() {
        try {
            JGDI j = JGDIFactory.newSynchronizedInstance(bootstrapURL);

            if (!isQmasterAlive) {
                System.err.println(new Date() + ": S U C C E S S: Qmaster restored");
                isQmasterAlive = true;
                jgdi = j;
                registerListeners();
            } else {
                j.close();
            }
            return;
        } catch (Exception ex) {
            // we don't need to print errors when qmaster is down
            //ex.printStackTrace();
        }

        if (isQmasterAlive) {
            System.err.println(new Date() + ": W A R N I N G: Qmaster CRASH detected");
            isQmasterAlive = false;
            try {
                finishEventClient.close();
                modEventClient.close();
                downEventClient.close();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    public synchronized void waitForQmasterAlive() {
        pingQmaster();
        while (!isQmasterAlive) {
            try {
                Thread.sleep(PING_QMASTER_INTERVAL_MS);
            } catch (Exception ex) {
                // do nothing
            }
        }
    }

    @Override
    public String submitJob(GridJobSubmitInfo gji) {
        String ret = "";
        Process process = null;
        int offset = 9;

        long sleepTime = 2000;
        boolean needsContinue = false;
        String err = "";
        int i = 1;
        do {
            if (i > 1) {
                try {
                    Thread.sleep(sleepTime);
                } catch (Exception ex) {
                    // nothing to report
                }
            }


            try {
                // Make the qsub command
                StringBuilder cmd = new StringBuilder();

                String username = gji.getUsername();
                if (username == null) {
                    throw new Exception("Failed to get Username");
                }

                // Add sudo -u username if privilegeEscalation is set to true
                if (gji.getPrivilegeEscalation()) {
                    cmd.append("sudo -E -u ");
                    cmd.append(username);
                    cmd.append(" ");
                }

                String executableLocation = gji.getCommand();

                if (executableLocation == null) {
                    throw new Exception("Failed to get Executable Location");
                }

                List<String> arguments = gji.getArguments();

                if (arguments == null || arguments.contains(null)) {
                    throw new Exception("Failed to get command line arguments");
                }

                cmd.append("qsub ");    // qsub command

                if (gji.getSubmissionType() == GridJobSubmitInfo.SUBMISSION_ARRAY) {
                    cmd.append(gji.getCommand());   // executable
                    offset += 6;
                } else {
                    cmd.append(gji.getNativeSpecification()); // qsub arguments
                    cmd.append(" -o ");                             // output stream path flag
                    cmd.append(gji.getOutputPath());    // output stream path
                    cmd.append(" -e ");                             // error stream path flag
                    cmd.append(gji.getErrorPath());     // error stream path


                    Properties envProperties = gji.getEnvironmentProperties();
                    String[] environment = null;

                    if (envProperties != null) {
                        environment = new String[envProperties.size()];
                        int ei = 0;
                        for (String varName : envProperties.stringPropertyNames()) {
                            StringBuilder sb = new StringBuilder(varName);
                            sb.append("=");
                            sb.append(envProperties.getProperty(varName));
                            environment[ei++] = sb.toString();
                        }
                    }

                    if (environment != null && environment.length > 0) {
                        cmd.append(" -v ");
                        int s_index = 0;
                        for (String s : environment) {
                            if (s_index > 0) {
                                cmd.append(",");
                            }

                            cmd.append(s);
                            s_index++;
                        }

                        cmd.append(" ");
                    }

                    cmd.append(" ");

                    cmd.append(gji.getCommand());   // executable

                    // Arguments of executable
                    for (String arg : arguments) {
                        cmd.append(" ");
                        cmd.append(arg);
                    }
                }

                if (!isQmasterAlive()) {
                    waitForQmasterAlive();
                    needsContinue = true;
                    continue;
                }

                StringTokenizer st = new StringTokenizer(cmd.toString());
                String[] command = new String[st.countTokens()];

                for (int k = 0; k < command.length; k++) {
                    command[k] = st.nextToken();
                }

                ProcessBuilder pb = new ProcessBuilder(command);
                Map<String, String> env = pb.environment();

                Properties props = gji.getEnvironmentProperties();

                if (props != null) {
                    for (String varName : props.stringPropertyNames()) {
                        env.put(varName, props.getProperty(varName));
                    }
                }

                process = pb.start();

                // Read process Id from the InputStream
                byte[] buff = new byte[100];
                byte[] errBuff = new byte[255];
                process.getInputStream().read(buff);
                process.getErrorStream().read(errBuff);

                err = new String(errBuff);
                releaseProcess(process);
                process = null;


                if (err.contains("can't connect to service") || err.contains("got read error")) {
                    waitForQmasterAlive();
                    needsContinue = true;
                    continue;
                }

                String response = new String(buff);

                for (String s : response.split("\n")) {
                    if (s.startsWith("Your job")) {
                        String sub = s.substring(offset);
                        int spaceIndex = sub.indexOf(" ");

                        if (spaceIndex != -1) {
                            return sub.substring(0, spaceIndex);
                        }
                        break;
                    }
                }

                // we should NOT be here if the has been successfully submitted. 
                if (err.trim().length() > 0) {
                    err += "\n";
                }

                err += "ERROR: " + response;
            } catch (Exception ex) {
                ex.printStackTrace();
                StringBuilder errorMsg = new StringBuilder("Unable to submit job. Internal error occurred\n");

                errorMsg.append("\n     Date: ").append(new Date().toString());
                errorMsg.append("\n   Reason: ").append(ex.getMessage());

                err = errorMsg.toString();
            } finally {
                if (process != null) {
                    releaseProcess(process);
                    process = null;
                }
            }

            System.err.println(new Date() + ": Attempt " + i + ": ERROR While submitting job: " + err);

            ret += "Attempt " + (i++) + ": " + err + "\n";
            if (i < 6) {
                needsContinue = true;
                if (i > 2) {
                    sleepTime *= 2; // multiple sleep time
                }
                continue;
            } else {
                return "ERROR:" + ret;
            }
        } while (needsContinue);

        return "ERROR:" + ret;
    }

    @Override
    public List<GridJobInfo> getJobList(String complexVariables) {
        List<GridJobInfo> ret = new LinkedList<GridJobInfo>();

        if (jgdi == null) {
            return null;
        }

        try {
            List<Job> qJobs = jgdi.getJobList(); // Get current jobs from JGDI

            Map<String, String> complexVars = new HashMap<String, String>();

            if (complexVariables != null) {
                String[] complexVarStringTokens = complexVariables.split(",");

                for (String str : complexVarStringTokens) {
                    if (str.trim().length() > 0) {
                        String[] var = str.trim().split("=");
                        if (var.length == 1) {
                            if (var[0].trim().length() > 0) {
                                complexVars.put(var[0], "true");
                            }
                        } else if (var.length == 2) {
                            if (var[0].trim().length() > 0) {
                                if (var[1].trim().length() > 0) {
                                    complexVars.put(var[0].trim(), var[1].trim());
                                } else {
                                    complexVars.put(var[0].trim(), "true");
                                }
                            }
                        }
                    }
                }
            }

            List<Job> plJobs = new LinkedList<Job>();

            for (Job j : qJobs) {

                boolean sameComplexVars = true;

                if (!complexVars.isEmpty()) {

                    // If instead of complex variables, admin preferred to set a prefix on each job's name
                    // submitted by pipeline. So we need to return only those jobs which name's start with
                    // specified prefix.
                    String prefix = complexVars.get("GridJobNamePrefix");

                    if (prefix != null) {
                        if (!j.getJobName().startsWith(prefix)) {
                            sameComplexVars = false;
                        }
                    } else {  // otherwise, return those jobs which have the required complex variables.
                        Map<String, String> jobComplexVars = new HashMap<String, String>();

                        for (int i = 0; i < j.getHardResourceCount(); i++) {
                            ComplexEntry ce = j.getHardResource(i);
                            String name = ce.getName();
                            String val = ce.getStringval();
                            jobComplexVars.put(name, val);
                        }

                        for (String cvn : complexVars.keySet()) {
                            if (!jobComplexVars.containsKey(cvn)) {
                                sameComplexVars = false;
                                break;
                            } else {
                                String jcvv = jobComplexVars.get(cvn);

                                if (!jcvv.equalsIgnoreCase(complexVars.get(cvn))) {
                                    sameComplexVars = false;
                                    break;
                                }
                            }
                        }
                    }
                }


                if (sameComplexVars) {
                    plJobs.add(j);
                }
            }

            for (Job j : plJobs) {

                String jobId = String.valueOf(j.getJobNumber());

                List<JobTask> taskList = j.getJaTasksList();

                if (taskList != null) {
                    if (taskList.size() == 1) {
                        JobTask task = taskList.get(0);
                        int taskNum = task.getTaskNumber();

                        GridJobInfo gji = getJobInfo(jobId + "." + taskNum, j);

                        if (gji != null) {
                            ret.add(gji);
                        }
                    } else if (taskList.isEmpty()) {
                        // When jobs are queued, their taskList is empty, we need to dig their structure and find out 
                        // the range of the tasks which needs to be executed. 
                        Range range = j.getJaStructure(0);

                        for (int i = range.getMin(); i <= range.getMax(); i += range.getStep()) {
                            ret.add(getJobInfo(jobId + "." + i, j));
                        }
                    } else {
                        for (JobTask task : taskList) {
                            GridJobInfo gji = getJobInfo(jobId + "." + task.getTaskNumber(), j);
                            if (gji != null) {
                                ret.add(gji);
                            }
                        }
                    }
                }
            }

        } catch (JGDIException ex) {
            ex.printStackTrace();
        }

        return ret;
    }

    private GridJobInfo getJobInfo(String jobId, Job j) {
        GridJobInfo gji = new GridJobInfo(jobId);

        int taskId = 1;

        if (jobId.contains(".")) {
            String strTaskId = jobId.substring(jobId.indexOf(".") + 1);
            taskId = Integer.valueOf(strTaskId);
            jobId = jobId.substring(0, jobId.indexOf("."));
        }


        try {
            if (j == null) {
                j = jgdi.getJob(Integer.valueOf(jobId));
                if (j == null) {
                    return null;
                }
            }

            for (int i = 0; i < j.getHardResourceCount(); i++) {
                ComplexEntry ce = j.getHardResource(i);

                gji.addComplexVariable(ce.getName(), ce.getStringval());
            }

            if (j.getJaTasksCount() == 0) {
                gji.setQueuedTime((long) j.getSubmissionTime() * 1000L);
                gji.setState(GridJobInfo.STATE_QUEUED);
            } else {
                int maxTaskId = 0;
                for (JobTask jt : j.getJaTasksList()) {
                    int taskNum = jt.getTaskNumber();

                    if (taskNum == taskId) {
                        gji.setStartTime((long) jt.getStartTime() * 1000L);
                        gji.setState(GridJobInfo.STATE_RUNNING);
                        return gji;
                    }

                    if (taskNum > maxTaskId) {
                        maxTaskId = taskNum;
                    }
                }


                // SGE starts to run jobs in order, which means that if Job 5 is running, then 1-4 are running
                // or have completed already. If we reach this part and the task was not listed j.getJaTasksList()
                // then job should be already finished. 
                if (taskId <= maxTaskId) {
                    return null;
                }

                // In cases where the task number is >maxTaskId, which can happen for example
                // in case when task 5 is completed in jobs with tasks 1-5 and pipeline wants to
                // know more about it when task 4 is running, then it will be hard to tell whether
                // task 5 has been finished or is queued, that's why we can ask ARCo database to
                // help us with this. If job is found in ARCo database, then it has been finished. So
                // we need to return null to tell pipeline that the requested job is not an active job.

                if (getFinishedJobInfo(jobId + "." + taskId) != null) {
                    return null;
                }

                gji.setQueuedTime((long) j.getSubmissionTime() * 1000L);
                gji.setState(GridJobInfo.STATE_QUEUED);

            }
        } catch (JGDIException ex) {

            ex.printStackTrace();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return gji;
    }

    @Override
    public GridJobInfo getJobInfo(String jobId) {
        return getJobInfo(jobId, null);
    }

    @Override
    public FinishedJobInfo getFinishedJobInfo(String jobId) throws PLGrid_InvalidMethodException {
        FinishedJobInfo fji = null;
        if (finishedJobRetrievalMethod == null || finishedJobRetrievalMethod.trim().length() == 0) {
            fji = sgeAccountingThread.getFinishedJobInfo(jobId);
        } else if (finishedJobRetrievalMethod.toLowerCase().equals("arco")) {
            if (arcoDatabase == null) {
                arcoDatabase = new ARCODatabase(this);
            }
            fji = arcoDatabase.getFinishedJobInfo(jobId);
        } else {
            throw new PLGrid_InvalidMethodException("Method \"" + finishedJobRetrievalMethod + "\" is not supported by this plugin for obtaining finished job information.");
        }

        return fji;
    }

    @Override
    public void killJob(String jobId, String username, boolean force) {
        if (username != null) {

            StringBuilder cmd = new StringBuilder();

            cmd.append("sudo -u ");
            cmd.append(username);
            cmd.append(" ");

            cmd.append("qdel ");    // qdel command

            if (force) {
                cmd.append("-f ");
            }

            cmd.append(jobId);

            Process process = null;

            try {
                process = Runtime.getRuntime().exec(cmd.toString());
                process.waitFor();
            } catch (Exception ex) {
                ex.printStackTrace();
                StringBuilder errorMsg = new StringBuilder("Unable to delete job ");

                errorMsg.append(jobId);
                errorMsg.append("\n     Date: ").append(new Date().toString());
                errorMsg.append("\n   Reason: ").append(ex.getMessage());

                System.err.println(errorMsg);

            } finally {
                if (process != null) {
                    releaseProcess(process);
                    process = null;
                }
            }
        } else {
            try {
                if (jobId.contains(".")) {
                    List<JGDIAnswer> answers = new LinkedList<JGDIAnswer>();
                    String[] tasks = new String[]{jobId};
                    jgdi.deleteJobsWithAnswer(tasks, force, null, answers);
                } else {
                    jgdi.deleteJob(Integer.valueOf(jobId));
                }
            } catch (Exception ex) {
                if (!ex.getMessage().contains("does not exist")) {
                    ex.printStackTrace();
                }
            }
        }
    }

    private void releaseProcess(Process p) {
        Exception ex = null;

        if (p != null) {
            try {
                p.getInputStream().close();
            } catch (Exception iex) {
                ex = iex;
            }
            try {
                p.getOutputStream().close();
            } catch (Exception oex) {
                ex = oex;
            }
            try {
                p.getErrorStream().close();
            } catch (Exception eex) {
                ex = eex;
            }
            try {
                p.destroy();
            } catch (Exception dex) {
                ex = dex;
            }
        }

        if (ex != null) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println("PipelineGridPlugin version " + PipelineGridPlugin.VERSION);
        System.out.println("JGDI Plugin version " + JGDI_PLUGIN_VERSION);
    }
    private final String SGE_ROOT;
    private final String SGE_CELL;
    private final String SGE_PORT;
    private SGEAccountingThread sgeAccountingThread;
    private String finishedJobRetrievalMethod;
    private ARCODatabase arcoDatabase;
    public static final String JGDI_PLUGIN_VERSION = "2.1";
    private static final int PING_QMASTER_INTERVAL_MS = 15000;
    private String bootstrapURL;
    private JGDI jgdi = null;
    private JGDIJobFinishListener jobFinishListener;
    private JGDIJobModListener jobModListener;
    private JGDIQmasterDownListener qmasterDownListener;
    private EventClient modEventClient;
    private EventClient finishEventClient;
    private EventClient downEventClient;
    private boolean isQmasterAlive = true;
    private Timer heartBeatTimer;
}
