const functions = require("firebase-functions");
const {BigQuery} = require("@google-cloud/bigquery");
const bigquery = new BigQuery();
const moment = require("moment-timezone");
const admin = require("firebase-admin");
admin.initializeApp();

exports.processDataAndInsertIntoFirestore = functions
    .region("northamerica-northeast1")
    .https.onCall(async (data, context) => {
      const userSearchId = data.userSearchId;
      const reportJson = data.reportJson;
      const db = admin.firestore();

      // Retrieve data from the userSearch collection
      const userSearchDoc = await db.collection("userSearch")
          .doc(userSearchId).get();
      if (!userSearchDoc.exists) {
        throw new functions.https.HttpsError("not-found", "Document not found");
      }
      const userSearchData = userSearchDoc.data();

      // Recover the user"s email address
      const userDoc = await db.collection("user")
          .doc(userSearchData.userId).get();
      if (!userDoc.exists) {
        throw new functions.https.HttpsError("not-found", "User not found");
      }
      const userEmail = userDoc.data().email;

      // Generate ProcessUID
      const processUID = admin.firestore().collection("dummy").doc().id;

      // Extract campaignId from object array in userSearchData
      const campaignIds = userSearchData.campaignId.map(
          (obj) => obj.campaignId);

      // Converting date strings into Date objects
      const startDate = moment.tz(
          userSearchData.startDate,
          "YYYY/MM/DD",
          "America/Montreal",
      ).startOf("day").toDate();
      const endDate = moment.tz(
          userSearchData.endDate,
          "YYYY/MM/DD",
          "America/Montreal",
      ).endOf("day").toDate();
      const yesterday = moment.tz("America/Montreal").subtract(1, "days")
          .endOf("day").toDate();
      const twoDaysAgo = moment.tz("America/Montreal").subtract(2, "days")
          .endOf("day").toDate();
      const eightDaysAgo = moment.tz("America/Montreal")
          .subtract(8, "days").startOf("day").toDate();

      // Calculation of total number of days and days elapsed
      const totalDays = (endDate - startDate) / (1000 * 60 * 60 * 24) + 1;
      const elapsedDays = (yesterday - startDate) / (1000 * 60 * 60 * 24) + 1;

      // Calculating the percentage of days spent
      let percDaysPassed = totalDays > 0 ? (elapsedDays / totalDays) * 100 : 0;
      percDaysPassed = Math.round(
          (percDaysPassed + Number.EPSILON) * 100
      ) / 100;

      // Calculate total and daily campaign revenue metrics
      let campaignCost = 0;
      let yesterdayCampaignCost = 0;
      let sevdaysRevenueTotal = 0;
      let sevdaysCount = 0;
      let yesterdaySpent = 0;

      reportJson.forEach((entry) => {
        const entryDate = moment.tz(
            entry["Date"],
            "YYYY/MM/DD",
            "America/Montreal",
        ).toDate();
        const revenue = parseFloat(entry["Revenue (Adv Currency)"]);

        if (
          entryDate >= startDate &&
          entryDate <= yesterday && !isNaN(revenue)
        ) {
          campaignCost += revenue;
        }

        if (
          entryDate >= startDate &&
          entryDate <= twoDaysAgo && !isNaN(revenue)
        ) {
          yesterdayCampaignCost += revenue;
        }

        if (
          entryDate >= eightDaysAgo &&
          entryDate <= yesterday && !isNaN(revenue)
        ) {
          sevdaysRevenueTotal += revenue;
          sevdaysCount++;
        }

        if (entryDate.getTime() === yesterday.getTime() && !isNaN(revenue)) {
          yesterdaySpent += revenue;
        }
      });

      let sevdaysAverageCampaignCost = sevdaysCount > 0 ?
          sevdaysRevenueTotal / Math.min(sevdaysCount, 7) : 0;
      sevdaysAverageCampaignCost = Math.round(
          (sevdaysAverageCampaignCost + Number.EPSILON) * 100
      ) / 100;

      // Calculate the percentage of budget spent
      const percBudgetSpent = userSearchData.budget > 0 ?
          Math.round(((campaignCost / userSearchData.budget) +
          Number.EPSILON) * 100) / 100 : 0;

      // Calculate the estimated cost
      const currentDate = moment.tz("America/Montreal").toDate();
      const daysLeft = (endDate - currentDate) /
          (1000 * 60 * 60 * 24) + 2;

      const estimatedCost = startDate === moment(currentDate)
          .format("YYYY/MM/DD") ? 0 :
          (userSearchData.budget - yesterdayCampaignCost) /
          (daysLeft > 0 ? daysLeft : 1);

      // Calculate the daily and yesterday estimated cost
      const dailyEstimatedCost = (userSearchData.budget - campaignCost) /
          (daysLeft - 1 > 0 ? daysLeft - 1 : 1);
      const yesterdaySailyEstimatedCost = (userSearchData.budget -
          yesterdayCampaignCost) /
          (daysLeft > 0 ? daysLeft : 1);

      // Composing the metrics table
      const metricsTable = {
        ProcessUID: processUID,
        CreationTimestamp: admin.firestore.FieldValue.serverTimestamp(),
        CreatedBy: userEmail,
        ClientName: userSearchData.partner.displayName,
        CampaignName: userSearchData.campaignName,
        StartDate: userSearchData.startDate,
        EndDate: userSearchData.endDate,
        Platform: "DV360",
        Budget: userSearchData.budget,
        perc_budget_spent: percBudgetSpent,
        CampaignID: campaignIds.join(";"),
        AdsetID: null,
        perc_days_passed: percDaysPassed,
        campaign_cost: campaignCost,
        yesterday_campaign_cost: yesterdayCampaignCost,
        sevdays_average_campaign_cost: sevdaysAverageCampaignCost,
        yesterday_spent: yesterdaySpent,
        estimated_cost: estimatedCost,
        daily_estimated_cost: dailyEstimatedCost,
        yesterday_daily_estimated_cost: yesterdaySailyEstimatedCost,
      };

      // Get data in BigQuery
      async function getRefAlertsDetails(errorIDs, query) {
        const options = {
          query: query,
          params: {errorIDs: errorIDs},
        };

        const [rows] = await bigquery.query(options);
        return rows;
      }


      // BUDGET ALERTS


      const calculateBudgetAlerts = (metricsTable) => {
        let errorID = null;
        const deltaValue = Math.abs(
            metricsTable.campaign_cost - metricsTable.Budget
        );

        if (metricsTable.perc_days_passed < 100) {
          if (deltaValue === 0) {
            errorID = "1d3ba4fd";
          } else if (metricsTable.campaign_cost > metricsTable.Budget) {
            errorID = "4566bc06";
          }
        } else {
          if (deltaValue === 0) {
            errorID = "18ade8c8";
          } else if (metricsTable.campaign_cost > metricsTable.Budget) {
            errorID = "0dae6c23";
          } else if (metricsTable.campaign_cost < metricsTable.Budget) {
            errorID = "c83e8c86";
          }
        }

        return {
          ...metricsTable,
          error_ID: errorID,
          delta_value: Math.round(deltaValue * 100) / 100,
        };
      };

      const budgetAlertsErrorTable = calculateBudgetAlerts(metricsTable);

      let query = `
      SELECT * FROM \`masterbackend.GR_Alerts_config.Ref_Alerts_Table\`
      WHERE error_id IN UNNEST(@errorIDs)`;

      let refAlertsDetails = await getRefAlertsDetails(
          budgetAlertsErrorTable.map((b) => b.error_ID), query);

      if (refAlertsDetails && refAlertsDetails.length > 0) {
        const refAlertDetails = refAlertsDetails[0];

        const enrichedData = budgetAlertsErrorTable.map((baet) => {
          const refAlertDetail = refAlertDetails.find(
              (r) => r.error_id === baet.error_ID);
          return {
            ...baet,
            error_platform: refAlertDetail.error_platform,
            error_platform_level: refAlertDetail.error_platform_level,
            error_pillar: refAlertDetail.error_pillar,
            error_pillar_type: refAlertDetail.error_pillar_type,
            error_metric: refAlertDetail.error_metric,
            error_metric_defintion: refAlertDetail.error_metric_defintion,
            error_metric_category: refAlertDetail.error_metric_category,
            error_rule: refAlertDetail.error_rule,
            error_rule_timeframe: refAlertDetail.error_rule_timeframe,
            error_rule_status: refAlertDetail.error_rule_status,
            error_rule_message: refAlertDetail.error_rule_message,
            error_rule_score: refAlertDetail.error_rule_score,
          };
        });

        // Insertion into Firestore
        await db.collection("Pacing_alerts_interim").doc().set(enrichedData);
      }


      // CAMPAIGN COST


      const campaignCostErrorTable = {
        ProcessUID: metricsTable.ProcessUID,
        CreationTimestamp: metricsTable.CreationTimestamp,
        CreatedBy: metricsTable.CreatedBy,
        ClientName: metricsTable.ClientName,
        CampaignName: metricsTable.CampaignName,
        StartDate: metricsTable.StartDate,
        EndDate: metricsTable.EndDate,
        Platform: metricsTable.Platform,
        Budget: metricsTable.Budget,
        perc_budget_spent: metricsTable.perc_budget_spent,
        CampaignID: metricsTable.CampaignID,
        perc_days_passed: metricsTable.perc_days_passed,
        campaign_cost: metricsTable.campaign_cost,
        yesterday_spent: metricsTable.yesterday_spent,
        estimated_cost: metricsTable.estimated_cost,
        daily_estimated_cost: metricsTable.daily_estimated_cost,
      };

      // Get error ID and delta value
      function determineErrorIdAndDeltaValue(
          campaignCost, budget, estimatedCost, percDaysPassed
      ) {
        let errorId = "null";
        let deltaValue = 0;

        const costDifference = campaignCost - estimatedCost;
        const absoluteDifferencePercentage = Math.round(
            Math.abs(costDifference / estimatedCost) * 100);

        if (campaignCost < budget && percDaysPassed < 1) {
          if (
            absoluteDifferencePercentage === 0 ||
            (campaignCost === 0 && estimatedCost === 0)
          ) {
            errorId = "d58127f6";
          } else if (
            campaignCost > estimatedCost &&
            campaignCost < (estimatedCost + estimatedCost * 0.05)
          ) {
            errorId = "7b217b04";
          } else if (
            campaignCost <= estimatedCost &&
            campaignCost > (estimatedCost - estimatedCost * 0.05)
          ) {
            errorId = "9861010f";
          } else if (
            campaignCost >= (estimatedCost + estimatedCost * 0.05) &&
            campaignCost < (estimatedCost + estimatedCost * 0.1)
          ) {
            errorId = "2d027066";
          } else if (
            campaignCost <= (estimatedCost - estimatedCost * 0.05) &&
            campaignCost > (estimatedCost - estimatedCost * 0.1)
          ) {
            errorId = "29f12f5f";
          } else if (campaignCost >= (estimatedCost + estimatedCost * 0.1)) {
            errorId = "6eee195a";
          } else if (campaignCost <= (estimatedCost - estimatedCost * 0.1)) {
            errorId = "daded50d";
          }
        }

        deltaValue = absoluteDifferencePercentage;

        return {errorId, deltaValue};
      }

      const {errorId, deltaValue} = determineErrorIdAndDeltaValue(
          campaignCostErrorTable.campaign_cost,
          campaignCostErrorTable.Budget,
          campaignCostErrorTable.estimated_cost,
          campaignCostErrorTable.perc_days_passed
      );
      campaignCostErrorTable.error_ID = errorId;
      campaignCostErrorTable.delta_value = deltaValue;

      query = `
        SELECT * 
        FROM \`masterbackend.GR_Alerts_config.Ref_Alerts_Table\` 
        WHERE error_id IN UNNEST(@errorIDs)
      `;

      refAlertsDetails = await getRefAlertsDetails(
          [campaignCostErrorTable.error_ID], query
      );

      if (refAlertsDetails && refAlertsDetails.length > 0) {
        const refAlertDetails = refAlertsDetails[0];

        Object.assign(campaignCostErrorTable, {
          error_platform: refAlertDetails.error_platform,
          error_platform_level: refAlertDetails.error_platform_level,
          error_pillar: refAlertDetails.error_pillar,
          error_pillar_type: refAlertDetails.error_pillar_type,
          error_metric: refAlertDetails.error_metric,
          error_metric_definition: refAlertDetails.error_metric_definition,
          error_metric_category: refAlertDetails.error_metric_category,
          error_rule: refAlertDetails.error_rule,
          error_rule_timeframe: refAlertDetails.error_rule_timeframe,
          error_rule_status: refAlertDetails.error_rule_status,
          error_rule_message: refAlertDetails.error_rule_message,
          error_rule_score: refAlertDetails.error_rule_score
        });


        // Insertion into Firestore
        await db.collection("Pacing_alerts_interim").doc()
            .set(campaignCostErrorTable);
      }


      // YESTERDAY SPEND


      const yesterdaySpendErrorsTable = {
        ProcessUID: metricsTable.ProcessUID,
        CreationTimestamp: metricsTable.CreationTimestamp,
        CreatedBy: metricsTable.CreatedBy,
        ClientName: metricsTable.ClientName,
        CampaignName: metricsTable.CampaignName,
        StartDate: metricsTable.StartDate,
        EndDate: metricsTable.EndDate,
        Platform: metricsTable.Platform,
        Budget: metricsTable.Budget,
        perc_budget_spent: metricsTable.perc_budget_spent,
        CampaignID: metricsTable.CampaignID,
        perc_days_passed: metricsTable.perc_days_passed,
        campaign_cost: metricsTable.campaign_cost,
        yesterday_spent: metricsTable.yesterday_spent,
        estimated_cost: metricsTable.estimated_cost,
        daily_estimated_cost: metricsTable.daily_estimated_cost,
      };

      // Get error ID and delta value
      function determineYesterdayErrorIdAndDeltaValue(
          yesterdaySpent, yesterdayDailyEstimatedCost, percDaysPassed,
      ) {
        let errorId = "null";
        let deltaValue = 0;

        if (percDaysPassed < 1) {
          const difference = yesterdaySpent - yesterdayDailyEstimatedCost;
          const percentageDifference = Math.abs(difference /
          yesterdayDailyEstimatedCost) * 100;

          if (
            percentageDifference === 0 ||
            (yesterdaySpent === 0 && yesterdayDailyEstimatedCost === 0)
          ) {
            errorId = "3b642aa5";
          } else if (
            difference > 0 &&
            difference < yesterdayDailyEstimatedCost * 0.05
          ) {
            errorId = "b9fb7a17";
          } else if (
            difference <= 0 &&
            -difference < yesterdayDailyEstimatedCost * 0.05
          ) {
            errorId = "dfc012e0";
          } else if (
            difference >= yesterdayDailyEstimatedCost * 0.05 &&
            difference < yesterdayDailyEstimatedCost * 0.1
          ) {
            errorId = "605c135b";
          } else if (
            -difference >= yesterdayDailyEstimatedCost * 0.05 &&
            -difference < yesterdayDailyEstimatedCost * 0.1
          ) {
            errorId = "c16ceede";
          } else if (difference >= yesterdayDailyEstimatedCost * 0.1) {
            errorId = "d1380845";
          } else if (-difference >= yesterdayDailyEstimatedCost * 0.1) {
            errorId = "4a190115";
          }

          deltaValue = Math.round(percentageDifference);
        }

        return {errorId, deltaValue};
      }

      const {yesterdayErrorId, yesterdayDeltaValue} =
      determineYesterdayErrorIdAndDeltaValue(
          yesterdaySpendErrorsTable.yesterday_spent,
          yesterdaySpendErrorsTable.daily_estimated_cost,
          yesterdaySpendErrorsTable.perc_days_passed,
      );

      yesterdaySpendErrorsTable.error_ID = yesterdayErrorId;
      yesterdaySpendErrorsTable.delta_value = yesterdayDeltaValue;

      refAlertsDetails = await getRefAlertsDetails(
          [yesterdaySpendErrorsTable.error_ID], query
      );

      if (refAlertsDetails && refAlertsDetails.length > 0) {
        const refAlertDetails = refAlertsDetails[0];

        Object.assign(yesterdaySpendErrorsTable, {
          error_platform: refAlertDetails.error_platform,
          error_platform_level: refAlertDetails.error_platform_level,
          error_pillar: refAlertDetails.error_pillar,
          error_pillar_type: refAlertDetails.error_pillar_type,
          error_metric: refAlertDetails.error_metric,
          error_metric_definition: refAlertDetails.error_metric_definition,
          error_metric_category: refAlertDetails.error_metric_category,
          error_rule: refAlertDetails.error_rule,
          error_rule_timeframe: refAlertDetails.error_rule_timeframe,
          error_rule_status: refAlertDetails.error_rule_status,
          error_rule_message: refAlertDetails.error_rule_message,
          error_rule_score: refAlertDetails.error_rule_score
        });

        // Insertion into Firestore
        await db.collection("Pacing_alerts_interim")
            .doc().set(yesterdaySpendErrorsTable);
      }


      // SEVEN DAYS AVERAGE


      // Get error ID and delta value
      function determineSevdaysErrorIdAndDeltaValue(
          sevdaysAverageCampaignCost,
          yesterdayDailyEstimatedCost,
          budget,
          percDaysPassed,
      ) {
        let errorId = "null";
        let deltaValue = 0;

        if (sevdaysAverageCampaignCost < budget && percDaysPassed < 1) {
          const diff = sevdaysAverageCampaignCost - yesterdayDailyEstimatedCost;
          const percentageDiff = yesterdayDailyEstimatedCost !== 0 ?
          Math.abs(diff / yesterdayDailyEstimatedCost) * 100 : 0;

          if (
            percentageDiff === 0 ||
            (
              sevdaysAverageCampaignCost === 0 &&
              yesterdayDailyEstimatedCost === 0
            )
          ) {
            errorId = "2e093f9c";
          } else if (
            sevdaysAverageCampaignCost > yesterdayDailyEstimatedCost &&
            sevdaysAverageCampaignCost < yesterdayDailyEstimatedCost +
            yesterdayDailyEstimatedCost * 0.05
          ) {
            errorId = "0a7eb791";
          } else if (
            sevdaysAverageCampaignCost <= yesterdayDailyEstimatedCost &&
            sevdaysAverageCampaignCost > yesterdayDailyEstimatedCost -
            yesterdayDailyEstimatedCost * 0.05
          ) {
            errorId = "1d82c801";
          } else if (
            sevdaysAverageCampaignCost >= yesterdayDailyEstimatedCost +
            yesterdayDailyEstimatedCost * 0.05 &&
            sevdaysAverageCampaignCost < yesterdayDailyEstimatedCost +
            yesterdayDailyEstimatedCost * 0.1
          ) {
            errorId = "3397e6ab";
          } else if (
            sevdaysAverageCampaignCost <= yesterdayDailyEstimatedCost -
            yesterdayDailyEstimatedCost * 0.05 &&
            sevdaysAverageCampaignCost > yesterdayDailyEstimatedCost -
            yesterdayDailyEstimatedCost * 0.1
          ) {
            errorId = "81410757";
          } else if (
            sevdaysAverageCampaignCost >= yesterdayDailyEstimatedCost +
            yesterdayDailyEstimatedCost * 0.1
          ) {
            errorId = "c92c27e3";
          } else if (
            sevdaysAverageCampaignCost <= yesterdayDailyEstimatedCost -
            yesterdayDailyEstimatedCost * 0.1
          ) {
            errorId = "397a5257";
          }

          deltaValue = Math.round(percentageDiff);
        }

        return {errorId, deltaValue};
      }

      const {errorId: errorIdAvgSevdays, deltaValue: deltaValueAvgSevdays} =
          determineSevdaysErrorIdAndDeltaValue(
              sevdaysAverageCampaignCost,
              yesterdaySailyEstimatedCost,
              metricsTable.perc_days_passed,
          );
      const errorIDsAvgSevdays = [errorIdAvgSevdays];
      const refAlertsDetailsAvgSevdays = await getRefAlertsDetails(
          errorIDsAvgSevdays, query,
      );

      if (refAlertsDetailsAvgSevdays && refAlertsDetailsAvgSevdays.length > 0) {
        const refAlertDetailAvgSevdays = refAlertsDetailsAvgSevdays[0];

        const sevdaysErrorsTableData = {
          ...metricsTable,
          error_ID_avg_sevdays: errorIdAvgSevdays,
          delta_value: deltaValueAvgSevdays,
          ...refAlertDetailAvgSevdays,
        };

        // Insertion into Firestore
        await db.collection("Pacing_alerts_interim")
            .doc().set(sevdaysErrorsTableData);
      }

      // Update error rule message
      async function updateErrorRuleMessages() {
        const db = admin.firestore();
        const pacingAlertsInterimRef = db.collection("Pacing_alerts_interim");

        // Retrieve all documents in the Pacing_alerts_interim collection
        const snapshot = await pacingAlertsInterimRef.get();

        if (snapshot.empty) {
          console.log("No matching documents.");
          return;
        }

        // Browse each document to update error_rule_message if necessary
        snapshot.forEach((doc) => {
          const data = doc.data();
          if (data.error_rule_message && data.delta_value) {
            const updatedMessage = data.error_rule_message
                .replace(/X/g, data.delta_value.toString());

            // Update document with updated error message
            pacingAlertsInterimRef.doc(doc.id)
                .update({error_rule_message: updatedMessage})
                .then(
                    () => console.log(
                        `Document ${doc.id} updated successfully`
                    ),
                )
                .catch(
                    (error) => console.error(
                        `Error updating document ${doc.id}: `, error,
                    ),
                );
          }
        });
      }

      updateErrorRuleMessages();

      // Insert alert into collection
      async function insertIntoPacingAlertsDatamart() {
        const db = admin.firestore();
        const pacingAlertsInterimRef = db.collection("Pacing_alerts_interim");
        const pacingAlertsDatamartRef = db.collection("Pacing_alerts_datamart");

        const currentDate = moment.tz("America/Montreal").format("YYYY-MM-DD");

        // Retrieve documents from Pacing_alerts_interim that match the criteria
        const snapshot = await pacingAlertsInterimRef
            .where("ProcessDate", "==", currentDate)
            .where("Platform", "==", "DV360")
            .where("ProcessStatus", "==", "New")
            .get();

        if (snapshot.empty) {
          console.log("No matching documents in interim collection.");
          return;
        }

        snapshot.forEach(async (doc) => {
          const data = doc.data();

          // Check whether a corresponding document
          // already exists in Pacing_alerts_datamart
          const datamartSnapshot = await pacingAlertsDatamartRef
              .where("ProcessUID", "==", data.ProcessUID)
              .where("ProcessDate", "==", currentDate)
              .where("Platform", "==", data.Platform)
              .get();

          if (datamartSnapshot.empty) {
            // If no duplicates exist, insert document
            await pacingAlertsDatamartRef.add(data)
                .then(() => console.log(
                    `Inserted document into Pacing_alerts_datamart
                     for ProcessUID: ${data.ProcessUID}`
                ))
                .catch((error) => console.error(
                    "Error inserting document: ", error
                ));
          } else {
            console.log(
                `Document already exists in Pacing_alerts_datamart
                 for ProcessUID: ${data.ProcessUID}`
            );
          }
        });
      }

      insertIntoPacingAlertsDatamart();

      // Insert alert into collection
      async function insertIntoAlertsDatamart() {
        const db = admin.firestore();
        const pacingAlertsInterimRef = db.collection("Pacing_alerts_interim");
        const alertsDatamartRef = db.collection("Alerts_datamart");

        const currentDate = moment.tz("America/Montreal").format("YYYY-MM-DD");

        // Retrieve all documents from
        // Pacing_alerts_interim that match the criteria
        const snapshot = await pacingAlertsInterimRef
            .where("ProcessDate", "==", currentDate)
            .where("Platform", "==", "DV360")
            .where("ProcessStatus", "==", "New")
            .get();

        if (snapshot.empty) {
          console.log(
              "No matching documents in interim collection for Alerts datamart."
          );
          return;
        }

        snapshot.forEach(async (doc) => {
          const data = doc.data();

          // Check if a corresponding document already exists in Alerts_datamart
          const datamartSnapshot = await alertsDatamartRef
              .where("ProcessUID", "==", data.ProcessUID)
              .where("ProcessDate", "==", currentDate)
              .where("Platform", "==", data.Platform)
              .get();

          if (datamartSnapshot.empty) {
            // If no duplicates exist, insert document
            await alertsDatamartRef.add({
              ...data,
              platform_value: Number(data.platform_value),
              input_value: Number(data.input_value),
              delta_value: data.delta_value.toString(),
              AlertVisibility: true
            })
                .then(() => console.log(
                    `Inserted document into Alerts_datamart for
                     ProcessUID: ${data.ProcessUID}`
                ))
                .catch((error) => console.error(
                    "Error inserting document: ", error
                ));
          } else {
            console.log(
                `Document already exists in Alerts_datamart
                 for ProcessUID: ${data.ProcessUID}`
            );
          }
        });
      }

      insertIntoAlertsDatamart();
    });
