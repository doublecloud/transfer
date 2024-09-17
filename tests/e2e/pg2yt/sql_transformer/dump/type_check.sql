-- needs to be sure there is db1
create table __test (
    id  bigint not null,
    data json,
    primary key (id)
);


insert into __test (id, data) values (1, '{
   "eventVersion":"1.08",
   "eventTime":"2023-06-02T23:07:00Z",
   "sourceIPAddress":"cloudtrail.amazonaws.com",
   "userAgent":"cloudtrail.amazonaws.com",
   "requestParameters":{
      "bucketName":"yadc-org-aws-cloudtrail-logs",
      "Host":"yadc-org-aws-cloudtrail-logs.s3.eu-central-1.amazonaws.com",
      "acl":""
   },
   "responseElements":null,
   "additionalEventData":{
      "SignatureVersion":"SigV4",
      "CipherSuite":"ECDHE-RSA-AES128-GCM-SHA256",
      "bytesTransferredIn":0,
      "AuthenticationMethod":"AuthHeader",
      "x-amz-id-2":"4tzhNW47AgT+waPyPc61jNJbjU1UA/AFXy6LXXXrnJfwqNcTNzV5IaNMCvDNr0uCKXP8kczdevYbCkeZu8EOgA==",
      "bytesTransferredOut":480
   },
   "requestID":"Y8KCYBP618TEW221",
   "eventID":"00002c01-c658-418f-a1b2-c4dbc152d643",
   "readOnly":true,
   "resources":[
      {
         "ARN":"arn:aws:s3:::yadc-org-aws-cloudtrail-logs"
      }
   ],
   "eventType":"AwsApiCall",
   "managementEvent":true
}');
