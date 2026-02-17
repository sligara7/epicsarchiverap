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
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.config.PVTypeInfo;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 * Export a slimmed-down archival configuration for PVs on this instance.
 * Returns only the archival parameters needed for syncing between clusters.
 * Used by ExportArchivalConfig which fans out to all appliances.
 *
 * @author asligar
 */
public class ExportArchivalConfigForThisInstance implements BPLAction {
    private static Logger logger = LogManager.getLogger(ExportArchivalConfigForThisInstance.class.getName());

    @SuppressWarnings("unchecked")
    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {
        String identity = configService.getMyApplianceInfo().getIdentity();
        logger.info("Exporting slimmed-down archival config for instance " + identity);

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            out.println("[");
            boolean first = true;
            for (String pvName : configService.getPVsForThisAppliance()) {
                PVTypeInfo typeInfo = configService.getTypeInfoForPV(pvName);
                if (typeInfo != null) {
                    if (first) {
                        first = false;
                    } else {
                        out.println(",");
                    }
                    out.print(encodeArchivalConfig(typeInfo).toJSONString());
                } else {
                    logger.warn("Skipping PV with no type info: " + pvName + " on appliance " + identity);
                }
            }
            out.println();
            out.println("]");
        } catch (Exception ex) {
            logger.error("Exception exporting archival config for instance " + identity, ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    @SuppressWarnings("unchecked")
    static JSONObject encodeArchivalConfig(PVTypeInfo typeInfo) {
        JSONObject pvConfig = new JSONObject();
        pvConfig.put("pvName", typeInfo.getPvName());
        pvConfig.put("samplingPeriod", (double) typeInfo.getSamplingPeriod());
        pvConfig.put("samplingMethod", typeInfo.getSamplingMethod().toString());
        pvConfig.put("policyName", typeInfo.getPolicyName());

        String[] archiveFields = typeInfo.getArchiveFields();
        JSONArray fieldsArray = new JSONArray();
        if (archiveFields != null) {
            fieldsArray.addAll(Arrays.asList(archiveFields));
        }
        pvConfig.put("archiveFields", fieldsArray);

        pvConfig.put("usePVAccess", typeInfo.isUsePVAccess());
        pvConfig.put("paused", typeInfo.isPaused());
        return pvConfig;
    }
}
