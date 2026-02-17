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
import org.epics.archiverappliance.config.ApplianceAggregateInfo;
import org.epics.archiverappliance.config.ApplianceInfo;
import org.epics.archiverappliance.config.ConfigService;
import org.epics.archiverappliance.utils.ui.GetUrlContent;
import org.epics.archiverappliance.utils.ui.MimeTypeConstants;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.Map;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

/**
 *
 * @epics.BPLAction - Get a health summary for the entire cluster in a single call.
 * Returns overall cluster health, per-appliance status, and aggregated PV counts/rates.
 * @epics.BPLActionEnd
 *
 * @author asligar
 *
 */
public class GetClusterHealthSummary implements BPLAction {
    private static Logger logger = LogManager.getLogger(GetClusterHealthSummary.class.getName());

    @SuppressWarnings("unchecked")
    @Override
    public void execute(HttpServletRequest req, HttpServletResponse resp, ConfigService configService)
            throws IOException {

        resp.setContentType(MimeTypeConstants.APPLICATION_JSON);
        try (PrintWriter out = resp.getWriter()) {
            JSONObject result = new JSONObject();

            int totalAppliances = 0;
            int activeAppliances = 0;
            long totalPVs = 0;
            double totalEventRate = 0.0;
            double totalStorageRate = 0.0;
            Map<String, Long> pvsByStatus = new LinkedHashMap<>();
            JSONArray appliancesArray = new JSONArray();

            for (ApplianceInfo applianceInfo : configService.getAppliancesInCluster()) {
                totalAppliances++;
                String identity = applianceInfo.getIdentity();
                JSONObject applianceObj = new JSONObject();
                applianceObj.put("identity", identity);

                boolean reachable = isApplianceReachable(applianceInfo);
                applianceObj.put("reachable", reachable);

                if (reachable) {
                    activeAppliances++;
                    try {
                        ApplianceAggregateInfo aggregateInfo =
                                configService.getAggregatedApplianceInfo(applianceInfo);
                        if (aggregateInfo != null) {
                            long pvCount = (long) aggregateInfo.getTotalPVCount();
                            double eventRate = aggregateInfo.getTotalEventRate();
                            double storageRate = aggregateInfo.getTotalStorageRate();

                            applianceObj.put("pvCount", pvCount);
                            applianceObj.put("eventRate", eventRate);
                            applianceObj.put("storageRate", storageRate);

                            totalPVs += pvCount;
                            totalEventRate += eventRate;
                            totalStorageRate += storageRate;
                        } else {
                            applianceObj.put("pvCount", 0L);
                            applianceObj.put("eventRate", 0.0);
                            applianceObj.put("storageRate", 0.0);
                        }
                    } catch (Exception ex) {
                        logger.warn("Could not get aggregate info for appliance " + identity, ex);
                        applianceObj.put("pvCount", 0L);
                        applianceObj.put("eventRate", 0.0);
                        applianceObj.put("storageRate", 0.0);
                    }
                } else {
                    applianceObj.put("pvCount", 0L);
                    applianceObj.put("eventRate", 0.0);
                    applianceObj.put("storageRate", 0.0);
                }

                appliancesArray.add(applianceObj);
            }

            // Get PV status breakdown by querying each appliance's getPVStatus
            try {
                pvsByStatus = getPVStatusBreakdown(configService);
            } catch (Exception ex) {
                logger.warn("Could not get PV status breakdown", ex);
            }

            boolean clusterHealthy = (activeAppliances == totalAppliances) && (totalAppliances > 0);

            result.put("clusterHealthy", clusterHealthy);
            result.put("totalAppliances", totalAppliances);
            result.put("activeAppliances", activeAppliances);
            result.put("totalPVs", totalPVs);
            result.put("totalEventRate", totalEventRate);
            result.put("totalStorageRate", totalStorageRate);

            JSONObject pvsByStatusObj = new JSONObject();
            for (Map.Entry<String, Long> entry : pvsByStatus.entrySet()) {
                pvsByStatusObj.put(entry.getKey(), entry.getValue());
            }
            result.put("pvsByStatus", pvsByStatusObj);
            result.put("appliances", appliancesArray);

            out.println(result.toJSONString());
        } catch (Exception ex) {
            logger.error("Exception getting cluster health summary", ex);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
    }

    /**
     * Check if an appliance is reachable by pinging its engine URL.
     */
    private boolean isApplianceReachable(ApplianceInfo applianceInfo) {
        try {
            String engineURL = applianceInfo.getEngineURL() + "/getProcessMetrics";
            JSONObject content = GetUrlContent.getURLContentAsJSONObject(engineURL, false);
            return content != null;
        } catch (Exception ex) {
            logger.debug("Appliance " + applianceInfo.getIdentity() + " is not reachable", ex);
            return false;
        }
    }

    /**
     * Get a breakdown of PV counts by archival status across the cluster.
     */
    private Map<String, Long> getPVStatusBreakdown(ConfigService configService) {
        Map<String, Long> statusCounts = new LinkedHashMap<>();
        for (ApplianceInfo applianceInfo : configService.getAppliancesInCluster()) {
            try {
                String mgmtUrl = applianceInfo.getMgmtURL();
                String statusUrl = mgmtUrl + "/getPVsForThisAppliance";
                JSONArray pvList = GetUrlContent.getURLContentAsJSONArray(statusUrl, false);
                if (pvList != null) {
                    for (Object obj : pvList) {
                        JSONObject pvObj = (JSONObject) obj;
                        String status = (String) pvObj.get("status");
                        if (status != null) {
                            statusCounts.merge(status, 1L, Long::sum);
                        }
                    }
                }
            } catch (Exception ex) {
                logger.debug("Could not get PV status for appliance " + applianceInfo.getIdentity(), ex);
            }
        }
        return statusCounts;
    }
}
