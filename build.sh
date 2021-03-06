#!/bin/bash
 
CREDENTIALS="$HOME/.genny/sheets.googleapis.com-java-quickstart"
if [ ! -d "$CREDENTIALS" ]; then
   mkdir -p $CREDENTIALS
fi

cp src/main/resources/credentials/genny $HOME/.genny/sheets.googleapis.com-java-quickstart/StoredCredential

export GOOGLE_CLIENT_SECRET="{\"installed\":{\"client_id\":\"260075856207-9d7a02ekmujr2bh7i53dro28n132iqhe.apps.googleusercontent.com\",\"project_id\":\"genny-sheets-181905\",\"auth_uri\":\"https:\/\/accounts.google.com\/o\/oauth2\/auth\",\"token_uri\":\"https:\/\/accounts.google.com\/o\/oauth2\/token\",\"auth_provider_x509_cert_url\":\"https:\/\/www.googleapis.com\/oauth2\/v1\/certs\",\"client_secret\":\"vgXEFRgQvh3_t_e5Hj-eb6IX\",\"redirect_uris\":[\"urn:ietf:wg:oauth:2.0:oob\",\"http:\/\/localhost\"]}}"
#export GOOGLE_CLIENT_SECRET="{\"installed\":{\"client_id\":\"260075856207-9d7a02ekmujr2bh7i53dro28n132iqhe.apps.googleusercontent.com\",\"project_id\":\"genny-sheets-181905\",\"auth_uri\":\"https:\/\/accounts.google.com\/o\/oauth2\/auth\",\"token_uri\":\"https:\/\/accounts.google.com\/o\/oauth2\/token\",\"auth_provider_x509_cert_url\":\"https:\/\/www.googleapis.com\/oauth2\/v1\/certs\",\"client_secret\":\"vgXEFRgQvh3_t_e5Hj-eb6IX\",\"redirect_uris\":[\"urn:ietf:wg:oauth:2.0:oob\",\"http:\/\/localhost\"]}}"

export GOOGLE_HOSTING_SHEET_ID="1OQ3IUdKTCCN-qgMahaNfc3KFOc_iN8l2BAVx7-KdA0A"
 

mvn clean install -DskipTests=true
mvn eclipse:eclipse
