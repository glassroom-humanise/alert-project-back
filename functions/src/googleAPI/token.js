const functions = require("firebase-functions");
const axios = require('axios');

exports.exchangeTokens = functions.region("northamerica-northeast1")
    .https.onCall(async (data, context) => {
        const { code } = data;

        const googleClientId = functions.config().google.client_id;
        const googleClientSecret = functions.config().google.client_secret;
        const redirectUri = 'http://localhost:4200/profile';

        const tokenUrl = 'https://oauth2.googleapis.com/token';

        try {
          const response = await axios.post(tokenUrl, {
            code: code,
            client_id: googleClientId,
            client_secret: googleClientSecret,
            redirect_uri: redirectUri,
            grant_type: 'authorization_code',
          });

          return response.data;
        } catch (error) {
          console.error('Error exchanging tokens:', error.response ? error.response.data : error.message);
          throw new functions.https.HttpsError('internal', 'Failed to exchange tokens');
        }
    });


exports.refreshAccessToken = functions.region("northamerica-northeast1")
    .https.onCall(async (data, context) => {
        const { refreshToken } = data;
        var requestURL = "https://www.googleapis.com/oauth2/v3/token";

        const googleClientId = functions.config().google.client_id;
        const googleClientSecret = functions.config().google.client_secret;

        try {
          const response = await axios.post(requestURL, {
            refresh_token : refreshToken,
            client_id : googleClientId,
            client_secret : googleClientSecret,
            grant_type : "refresh_token"
          });

          return response.data;
        } catch (error) {
          console.error('Error refreshing tokens:', error.response ? error.response.data : error.message);
          throw new functions.https.HttpsError('internal', 'Failed to refresh token');
        }
    });