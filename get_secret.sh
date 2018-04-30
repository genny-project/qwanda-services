#!/bin/bash

export $(sudo cat $GENNY_CREDENTIALS/$CUSTOMER_CODE/conf.env )
export $(sudo cat $GENNY_CREDENTIALS/$CUSTOMER_CODE/conf.env )
echo $GOOGLE_CLIENT_SECRET
echo $GOOGLE_HOSTING_SHEET_ID
mvn clean install 
mvn eclipse:eclipse
