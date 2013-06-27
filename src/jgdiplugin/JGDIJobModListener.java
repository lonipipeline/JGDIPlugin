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
package jgdiplugin;

import com.sun.grid.jgdi.event.Event;
import com.sun.grid.jgdi.event.EventListener;
import com.sun.grid.jgdi.event.JobTaskModEvent;
import plgrid.event.EventFinished;
import plgrid.event.EventQueued;
import plgrid.event.EventRunning;

/**
 *
 * @author Petros Petrosyan
 */
public class JGDIJobModListener implements EventListener {

    private final JGDIPlugin plugin;

    public JGDIJobModListener(JGDIPlugin plugin) {
        this.plugin = plugin;
    }

    public void eventOccured(Event evt) {
        if (evt instanceof JobTaskModEvent) {
            try {
                JobTaskModEvent jtme = (JobTaskModEvent) evt;

                int state = jtme.get().getState();


                String jobId = String.valueOf(jtme.getJobId());
                String taskId = String.valueOf(jtme.getTaskNumber());
                
                switch (state) {
                    case STATE_RUNNING + STATE_DELETED: // dr
                        plugin.fireEvent(new EventFinished(String.valueOf(jobId), "0", jtme.getTimestamp() * 1000, -88));
                        break;
                    case STATE_RUNNING:
                        plugin.fireEvent(new EventRunning(jobId, taskId));
                        break;
                    case STATE_QUEUED:
                    case (STATE_QUEUED + STATE_WAITING):
                        plugin.fireEvent(new EventQueued(jobId, taskId));
                        break;
                    case (STATE_QUEUED + STATE_WAITING + STATE_ERROR): // Eqw
                        plugin.fireEvent(new EventQueued(jobId, taskId, "Eqw"));
                        break;
                    default:
                        System.out.println(" WARNING: Job " + jtme.getJobId() + " changed status to " + state);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }


        }
    }
    /*
     * Job states moved in from def.h
     */
//private static final int STATE_IDLE                      = 0x00000000;
////private static final int ENABLED                       = 0x00000008;
//private static final int STATE_HELD                      = 0x00000010;
//private static final int STATE_MIGRATING                 = 0x00000020;
    private static final int STATE_QUEUED = 0x00000040;
    public static final int STATE_RUNNING = 0x00000080;
//private static final int STATE_SUSPENDED                 = 0x00000100;
//private static final int STATE_TRANSFERING               = 0x00000200;
    private static final int STATE_DELETED = 0x00000400;
    private static final int STATE_WAITING = 0x00000800;
//private static final int STATE_EXITING                   = 0x00001000;
//private static final int STATE_WRITTEN                   = 0x00002000;
///* used in execd - job waits for getting its ASH/JOBID */
//private static final int STATE_WAITING4OSJID             = 0x00004000;
///* used in execd - shepherd reports job exit but there are still processes */
    private static final int STATE_ERROR = 0x00008000;
//private static final int STATE_SUSPENDED_ON_THRESHOLD    = 0x00010000;
///*
//   SGEEE: qmaster delays job removal till schedd
//   does no inter need this finished job
//*/
//private static final int STATE_FINISHED                  = 0x00010000;
///* used in execd to prevent slave jobs from getting started */
//private static final int STATE_SLAVE                     = 0x00020000;
//private static final int STATE_DEFERRED_REQ              = 0x00100000;
}
