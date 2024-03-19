const functions = require("firebase-functions");
const {BigQuery} = require("@google-cloud/bigquery");
const bigquery = new BigQuery();

exports.queryBigQuery = functions.region("northamerica-northeast1")
    .https.onCall(async (data, context) => {
      const query = `
        SELECT * FROM 
        \`masterbackend.GR_Alerts_datamart.Alerts_datamart\` LIMIT 10
      `;
      const options = {query: query};

      try {
        const [rows] = await bigquery.query(options);
        return rows;
      } catch (error) {
        console.error("ERROR:", error);
        throw new functions.https.HttpsError(
            "unknown", "An error occurred while querying BigQuery.",
        );
      }
    });