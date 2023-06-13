/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.transforms.WriteDataChangeRecordsToAvro.DataChangeRecordToAvroFn;
import com.google.cloud.teleport.v2.transforms.WriteDataChangeRecordsToJson.DataChangeRecordToJsonTextFn;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * The {@link FileFormatFactorySpannerChangeStreamsToPubSub} class is a
 * {@link PTransform} that
 * takes in {@link PCollection} of DataChangeRecords. The transform writes each
 * record to
 * PubsubMessage in user specified format.
 */
@AutoValue
public abstract class FileFormatFactorySpannerChangeStreamsToPubSub
    extends PTransform<PCollection<DataChangeRecord>, PCollection<byte[]>> {

  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(FileFormatFactorySpannerChangeStreamsToPubSub.class);

  private static final String NATIVE_CLIENT = "native_client";
  private static final String PUBSUBIO = "pubsubio";

  public static WriteToPubSubBuilder newBuilder() {
    return new AutoValue_FileFormatFactorySpannerChangeStreamsToPubSub.Builder();
  }

  protected abstract String outputDataFormat();

  protected abstract String projectId();

  protected abstract String pubsubAPI();

  protected abstract String pubsubTopicName();

  @Override
  public PCollection<byte[]> expand(PCollection<DataChangeRecord> records) {
    PCollection<byte[]> encodedRecords = null;

    /*
     * Calls appropriate class Builder to performs PTransform based on user provided
     * File Format.
     */
    switch (outputDataFormat()) {
      case "AVRO":
        AvroCoder<com.google.cloud.teleport.v2.DataChangeRecord> coder = AvroCoder
            .of(com.google.cloud.teleport.v2.DataChangeRecord.class);
        encodedRecords = records
            .apply(
                "Write DataChangeRecord into AVRO",
                MapElements.via(new DataChangeRecordToAvroFn()))
            .apply(
                "Convert encoded DataChangeRecord in AVRO to bytes to be saved into"
                    + " PubsubMessage.",
                ParDo.of(
                    // Convert encoded DataChangeRecord in AVRO to bytes that can be saved into
                    // PubsubMessage.
                    new DoFn<com.google.cloud.teleport.v2.DataChangeRecord, byte[]>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {
                        com.google.cloud.teleport.v2.DataChangeRecord record = context.element();
                        byte[] encodedRecord = null;
                        try {
                          encodedRecord = CoderUtils.encodeToByteArray(coder, record);
                        } catch (CoderException ce) {
                          throw new RuntimeException(ce);
                        }
                        context.output(encodedRecord);
                      }
                    }));
        sendToPubSub(encodedRecords);

        break;
      case "JSON":
        encodedRecords = records
            .apply(
                "Write DataChangeRecord into JSON",
                MapElements.via(new DataChangeRecordToJsonTextFn()))
            .apply(
                "Convert encoded DataChangeRecord in JSON to bytes to be saved into"
                    + " PubsubMessage.",
                ParDo.of(
                    new DoFn<String, byte[]>() {
                      @ProcessElement
                      public void processElement(ProcessContext context) {
                        String record = context.element();
                        byte[] encodedRecord = record.getBytes();
                        context.output(encodedRecord);
                      }
                    }));
        sendToPubSub(encodedRecords);
        break;

      default:
        final String errorMessage = "Invalid output format:"
            + outputDataFormat()
            + ". Supported output formats: JSON, AVRO";
        LOG.info(errorMessage);
        throw new IllegalArgumentException(errorMessage);
    }
    return encodedRecords;
  }

  private void sendToPubSub(PCollection<byte[]> encodedRecords) {
    String pubsubTopicName = pubsubTopicName();
    String pubsubAPI = pubsubAPI();
    String projectId = projectId();
    String outputPubsubTopic = "projects/" + projectId + "/topics/" + pubsubTopicName;

    if (pubsubAPI.equals(NATIVE_CLIENT)) {
      final PublishToPubSubDoFn publishToPubSubDoFn = new PublishToPubSubDoFn(projectId, pubsubTopicName);
      encodedRecords.apply(ParDo.of(publishToPubSubDoFn));
    } else if (pubsubAPI.equals(PUBSUBIO)) {
      PCollection<PubsubMessage> outputPubsubMessageCollection = convertByteArrayToPubsubMessage(encodedRecords);
      outputPubsubMessageCollection.apply(
          "Write to PubSub topic", PubsubIO.writeMessages().to(outputPubsubTopic));
    } else {
      final String apiErrorMessage = "Invalid api:" + pubsubAPI + ". Supported apis: pubsubio, native_client";
      throw new IllegalArgumentException(apiErrorMessage);
    }
  }

  /** Method that takes in byte arrays and outputs PubsubMessages. */
  private PCollection<PubsubMessage> convertByteArrayToPubsubMessage(
      PCollection<byte[]> encodedRecords) {
    PCollection<PubsubMessage> messageCollection = encodedRecords.apply(
        ParDo.of(
            new DoFn<byte[], PubsubMessage>() {
              @ProcessElement
              public void processElement(ProcessContext context) {
                byte[] encodedRecord = context.element();

                final Map<String, String> attributes = new HashMap<>();

                try {
                  JsonObject jsonObj = JsonParser.parseString(new String(encodedRecord)).getAsJsonObject();
                  if (jsonObj.has("modType")) {
                    String modType = jsonObj.get("modType").getAsString();
                    attributes.put("modType", modType);
                  }

                  final String tableName = jsonObj.get("tableName").getAsString();
                  Collection<String> ids = getIdsFromModsArray(jsonObj.get("mods").getAsJsonArray());

                  for (String id : ids) {
                    if ("models_owner".equals(tableName)) {
                      String chainId = getChainIdFromModelsOwnerId(id);
                      if (chainId != null) {
                        attributes.put(chainId, "");
                      }
                    } else if ("models_tokentransfer".equals(tableName)) {
                      String chainId = getChainIdFromTokenTransferId(id);
                      if (chainId != null) {
                        attributes.put(chainId, "");
                      }
                    } else if ("models_nftindexv3".equals(tableName)) {
                      String chainId = getChainIdFromNftIndexV3Id(id);
                      if (chainId != null) {
                        attributes.put(chainId, "");
                      }
                    } else if ("models_nftlisting".equals(tableName)) {
                      String marketplaceId = getMarketplaceIdFromNftListing(id);
                      if (marketplaceId != null) {
                        attributes.put("marketplaceId-" + marketplaceId, "");
                      }
                    }
                  }

                  if ("models_nftlistingevent".equals(tableName)) {
                    Collection<String> marketplaceIds = getMarketplaceIdsFromModsArray(
                        jsonObj.get("mods").getAsJsonArray());

                    for (String marketplaceId : marketplaceIds) {
                      attributes.put("marketplaceId-" + marketplaceId, "");
                    }

                  }
                } catch (Exception e) {
                  // Unable to parse JSON body; move forward without the modType attribute
                  e.printStackTrace();
                }

                PubsubMessage pubsubMessage = new PubsubMessage(encodedRecord,
                    attributes.isEmpty() ? null : attributes);
                context.output(pubsubMessage);
              }
            }));
    return messageCollection;

  }

  protected static Collection<String> getIdsFromModsArray(JsonArray modsArray) {
    Set<String> ids = new TreeSet<>();
    modsArray.forEach(mod -> {
      JsonObject modObj = mod.getAsJsonObject();
      if (modObj.has("keysJson")) {
        // Each key is encoded as a JSON object
        JsonObject keysJson = JsonParser.parseString(modObj.get("keysJson").getAsString())
            .getAsJsonObject();
        if (keysJson.has("id")) {
          String id = keysJson.get("id").getAsString();
          if (id != null) {
            ids.add(id);
          }
        }
      }
    });

    return ids;
  }

  protected static Collection<String> getMarketplaceIdsFromModsArray(JsonArray modsArray) {
    Set<String> marketplaceIds = new TreeSet<>();
    modsArray.forEach(mod -> {
      JsonObject modObj = mod.getAsJsonObject();
      if (modObj.has("newValuesJson")) {
        // Each key is encoded as a JSON object
        JsonObject newValuesJson = JsonParser.parseString(modObj.get("newValuesJson").getAsString())
            .getAsJsonObject();
        if (newValuesJson.has("marketplace_id")) {
          String marketplaceId = newValuesJson.get("marketplace_id").getAsString();
          if (marketplaceId != null) {
            marketplaceIds.add(marketplaceId);
          }
        }
      }
    });

    return marketplaceIds;
  }

  protected static String getChainIdFromModelsOwnerId(String id) {
    int firstUnderscore = id.indexOf("_");
    if (firstUnderscore > 0) {
      String nftId = id.substring(firstUnderscore + 1);
      return getChainIdFromNftId(nftId);
    }

    return null;
  }

  protected static String getChainIdFromNftId(String nftId) {
    String[] parts = nftId.split("_");
    if (parts.length > 1) {
      return parts[parts.length - 2];
    }

    return null;
  }

  protected static String getChainIdFromTokenTransferId(String id) {
    String[] parts = id.split("_");
    if (parts.length > 4) {
      return parts[parts.length - 3];
    }

    return null;
  }

  protected static String getChainIdFromNftIndexV3Id(String id) {
    String[] parts = id.split("\\.");
    if (parts.length > 0) {
      return parts[0];
    }

    return null;
  }

  protected static String getMarketplaceIdFromNftListing(String id) {
    String[] parts = id.split("_");
    if (parts.length == 3) {
      return parts[1];
    }

    return null;
  }

  /** Builder for {@link FileFormatFactorySpannerChangeStreamsToPubSub}. */
  @AutoValue.Builder
  public abstract static class WriteToPubSubBuilder {

    public abstract WriteToPubSubBuilder setOutputDataFormat(String value);

    public abstract WriteToPubSubBuilder setProjectId(String value);

    public abstract WriteToPubSubBuilder setPubsubAPI(String value);

    public abstract WriteToPubSubBuilder setPubsubTopicName(String value);

    abstract FileFormatFactorySpannerChangeStreamsToPubSub autoBuild();

    public FileFormatFactorySpannerChangeStreamsToPubSub build() {
      return autoBuild();
    }
  }
}
