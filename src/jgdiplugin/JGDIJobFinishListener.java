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

import com.sun.grid.jgdi.event.Event;
import com.sun.grid.jgdi.event.EventListener;
import com.sun.grid.jgdi.event.JobDelEvent;
import com.sun.grid.jgdi.event.JobFinalUsageEvent;
import plgrid.event.EventFinished;

/**
 *
 * @author Petros Petrosyan
 */
public class JGDIJobFinishListener implements EventListener {

    private JGDIPlugin plugin;

    public JGDIJobFinishListener(JGDIPlugin plugin) {
        this.plugin = plugin;
    }

    public void eventOccured(Event evt) {
        if (evt instanceof JobFinalUsageEvent) {
            try {
                JobFinalUsageEvent jfue = (JobFinalUsageEvent) evt;
                // Get the job Id
                String jobId = String.valueOf(jfue.getJobId());
                String taskId = String.valueOf(jfue.getTaskId());


                if (jfue.getLoadValueNames().size() != 0) {
                    // Get the exit status
                    Double dblExitStatus = jfue.getLoadValue("exit_status");

                    int exit_status = 0;
                    boolean failed = false;

                    if (dblExitStatus != null) {
                        exit_status = dblExitStatus.intValue();
                    } else {
                        System.err.println("ERROR: Failed to get exit status of job " + jobId);
                        failed = true;
                    }

                    long endTime = 0;
                    long startTime = 0;
                    // Get the finish time and start time
                    Double dblEndTime = jfue.getLoadValue("end_time");
                    Double dblStartTime = jfue.getLoadValue("start_time");

                    if (dblEndTime != null && dblStartTime != null) {
                        endTime = dblEndTime.longValue() * 1000;
                        startTime = dblStartTime.longValue() * 1000;
                    } else {
                        System.err.println("ERROR: Failed to get time of job " + jobId + " Not sending event. End time=" + dblEndTime + " Start time=" + dblStartTime);
                        failed = true;
                    }

                    if (endTime > 0 && !failed) {
                        plugin.sendEvent(new EventFinished(jobId, taskId, endTime, startTime, exit_status));
                    }
                } else {
                    System.err.println("ERROR: Job " + jobId + "." + taskId + " finished but doesn't have load values.");
                }

            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } else if (evt instanceof JobDelEvent) {
            JobDelEvent jde = (JobDelEvent) evt;

            plugin.sendEvent(new EventFinished(String.valueOf(jde.getJobNumber()), "0", jde.getTimestamp() * 1000, -88));
        }
    }
}
