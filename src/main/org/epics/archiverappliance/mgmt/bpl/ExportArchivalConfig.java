/*******************************************************************************
 * Copyright (c) 2011 The Board of Trustees of the Leland Stanford Junior University
 * as Operator of the SLAC National Accelerator Laboratory.
 * Copyright (c) 2011 Brookhaven National Laboratory.
 * EPICS archiver appliance is distributed subject to a Software License Agreement found
 * in file LICENSE that is included with this distribution.
 *******************************************************************************/
package org.epics.archiverappliance.mgmt.bpl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.epics.archiverappliance.common.BPLAction;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 *
 * @epics.BPLAction - Export a slimmed-down archival configuration for all PVs in the cluster.
 * Returns only the archival parameters needed for syncing PV configurations between clusters,
 * without appliance-specific fields like data store paths or host names.
 * @epics.BPLActionEnd
 *
 * @author asligar
 *
 */
public class ExportArchivalConfig implements BPLAction {
    private static Logger logger = LogManager.getLogger(ExportArchivalConfig.class.getName());

    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        logger.info("Exporting slimmed-down archival configuration for cluster");
        LinkedList<String> exportURLs = new LinkedList<>();
        for (ApplianceInfo info : configService.getAppliancesInCluster()) {
            String mgmtUrl = info.getMgmtURL();
            exportURLs.add(mgmtUrl + "/exportArchivalConfigForAppliance");
        }

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            GetUrlContent.combineJSONArraysAndPrintln(exportURLs, out);
        }
    }
}
