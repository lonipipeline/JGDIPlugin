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
import com.sun.grid.jgdi.event.QmasterGoesDownEvent;

/**
 *
 * @author Petros Petrosyan
 */
public class JGDIQmasterDownListener implements EventListener {

    private final JGDIPlugin plugin;

    public JGDIQmasterDownListener(JGDIPlugin plugin) {
        this.plugin = plugin;
    }

    public void eventOccured(Event evt) {
        if (evt instanceof QmasterGoesDownEvent) {
            plugin.pingQmaster();
        }
    }
}
