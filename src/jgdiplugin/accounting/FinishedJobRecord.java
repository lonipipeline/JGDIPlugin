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

import java.util.StringTokenizer;

/**
 *
 * @author Petros Petrosyan
 */
public class FinishedJobRecord {

    public String qname;            // token 1
    public String hostname;         // token 2
    public String group;            // token 3
    public String username;         // token 4
    public String job_name;         // token 5
    public String job_number;       // token 6
    public String account;          // token 7
    public int priority;            // token 8
    public long submission_time;    // token 9
    public long start_time;         // token 10
    public long end_time;           // token 11
    public boolean failed;          // token 12
    public int exit_status;         // token 13
    public String ru_wallclock;     // token 14
    public String ru_utime;         // token 15
    public String ru_stime;         // token 16
    public String ru_maxrss;        // token 17
    public String ru_ixrss;         // token 18
    public String ru_ismrss;        // token 19
    public String ru_idrss;         // token 20
    public String ru_isrss;         // token 21
    public String ru_minflt;        // token 22
    public String ru_majflt;        // token 23
    public String ru_nswap;         // token 24
    public String ru_inblock;       // token 25
    public String ru_oublock;       // token 26
    public String ru_msgsnd;        // token 27
    public String ru_msgrcv;        // token 28
    public String ru_nsignals;      // token 29
    public String ru_nvcsw;         // token 30
    public String ru_nivcsw;        // token 31
    public String project;          // token 32
    public String department;       // token 33
    public String granted_pe;       // token 34
    public String slots;            // token 35
    public int task_number;         // token 36
    public String cpu;              // token 37
    public String mem;              // token 38
    public String io;               // token 39
    public String category;         // token 40
    public String iow;              // token 41
    public String pe_taskid;        // token 42
    public String maxvmem;          // token 43
    public String arid;             // token 44
    public String ar_submission_time;// token 45

    public FinishedJobRecord(String str) {
        StringTokenizer st = new StringTokenizer(str, ":");

        int tokNumber = 0;
        while (st.hasMoreElements()) {
            tokNumber++;

            String val = st.nextToken();

            // We only get what we need, fields which are not used by
            // pipeline will not be initialized.
            switch (tokNumber) {
                case 1:
                    qname = val;
                    break;
                case 6:
                    job_number = val;
                    break;
                case 36:
                    task_number = Integer.parseInt(val);
                    break;
                case 10:
                    start_time = Long.parseLong(val) * 1000;
                    break;
                case 11:
                    end_time = Long.parseLong(val) * 1000;
                    break;
                case 13:
                    exit_status = Integer.parseInt(val);
                    break;
                case 15:
                    ru_utime = val;
                    break;
                case 16:
                    ru_stime = val;
                    break;
            }
        }
    }
}
