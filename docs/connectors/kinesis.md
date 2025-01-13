---
title: "Kinesis Data Streams connector"
description: "Configure the Kinesis Data Streams connector to transfer data from AWS Kinesis Data Streams with {{ DC }} {{ data-transfer-name }}"
---

# Kinesis Data Streams connector

[Amazon Kinesis Data Streams ![external link](../_assets/external-link.svg)](https://aws.amazon.com/kinesis/data-streams/)
is a serverless service for capturing, processing, and storing data streams of various scales.
The Kinesis Data Streams connector enables you
to transfer data from AWS Kinesis Data Streams with {{ DC }} {{ data-transfer-name }}.

You can use this connector in **source** endpoints.

## Source endpoint

{% list tabs %}

* Configuration

    To configure a Kinesis Data Streams source endpoint, provide the following settings:

    1. In **AWS region**, specify the region where your Kinesis Data Streams is deployed.

    1. In **Kinesis stream**, enter the stream name.

    1. In **Access key ID** and **Secret access key**,
        enter the credentials to access the stream.

    1. In **Conversion rules**, select how you want to transform the streamed data.

{% endlist %}
