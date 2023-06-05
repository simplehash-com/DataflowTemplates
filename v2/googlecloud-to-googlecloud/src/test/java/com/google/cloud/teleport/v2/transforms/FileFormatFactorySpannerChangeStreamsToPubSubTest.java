/*
 * Copyright (C) 2023 Google LLC
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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Test;

public class FileFormatFactorySpannerChangeStreamsToPubSubTest {

  @Test
  public void test_getChainIdFromNftId() {
    Assert.assertEquals(
        "evm-1",
        FileFormatFactorySpannerChangeStreamsToPubSub.getChainIdFromNftId(
            "0x06012c8cf97bead5deae237070f9587f8e7a266d_evm-1_1609764"));

    Assert.assertEquals(
        "solana-101",
        FileFormatFactorySpannerChangeStreamsToPubSub.getChainIdFromNftId(
            "1115Tg5qkcjvGmoYSxtNRZXNaQcSjZ1HDsG8Px6cQxf_solana-101_0"));

    Assert.assertEquals(
        "flow-m",
        FileFormatFactorySpannerChangeStreamsToPubSub.getChainIdFromNftId(
            "A.097bafa4e0b48eef.CharityNFT_flow-m_0"));
  }

  @Test
  public void test_getChainIdFromModelsOwner() {
    Assert.assertEquals(
        "evm-pg",
        FileFormatFactorySpannerChangeStreamsToPubSub.getChainIdFromModelsOwnerId(
            "0x000000000000000000000000000000000000000a_0x07515cbe746d569619dace60da2828355bf21255_evm-pg_10"));

    Assert.assertEquals(
        "evm-ar",
        FileFormatFactorySpannerChangeStreamsToPubSub.getChainIdFromModelsOwnerId(
            "0x0000000000000000000000000000000011111111_0xfb39b00439950e1062420b78b12d6cc63377b364_evm-ar_104 "));
  }

  @Test
  public void test_getChainIdFromTokenTransferId() {
    Assert.assertEquals(
        "solana-101",
        FileFormatFactorySpannerChangeStreamsToPubSub.getChainIdFromTokenTransferId(
            "1112AmWwo5cEx2nUCTen9uxUaUeXzKzXDREjidgx6e7VnstHZ4mLg1Vw436RvdPm3x8x2o45WqXByP4zDW38gS4_179531956_solana-101_0_0"));

    Assert.assertEquals(
        "flow-m",
        FileFormatFactorySpannerChangeStreamsToPubSub.getChainIdFromTokenTransferId(
            "0x00000730c609f8c0_85116eb36228ec0253cc4b1fe34e6944d407896f0ca26adbb4d79bf0626e423e_a6d964dec3da82ef185fa26bbf8a3d2302542929afdc33dd470c096e94f998f9_flow-m_750001_0"));

    Assert.assertEquals(
        "evm-56",
        FileFormatFactorySpannerChangeStreamsToPubSub.getChainIdFromTokenTransferId(
            "0x000000000000000000000000000000000000000b_0xc8b3760ef577d063ccb88fc426d0d61856e8cd79dc728d038006611f65ee94f8_0xfadaf0da9a2f1e76adac4cf07bc219be54a7fbbbcd5f1ad3b2185cb7f6f64932_evm-56_119_0"));
  }

  @Test
  public void test_getChainIdFromNftIndexV3Id() {
    Assert.assertEquals(
        "evm-pg",
        FileFormatFactorySpannerChangeStreamsToPubSub.getChainIdFromNftIndexV3Id(
            "evm-pg.0x000000000000c57cf0a1f923d44527e703f1ad70.000000000000000000000000000000005686786385366677572412502652901104648841196544"));

    Assert.assertEquals(
        "flow-m",
        FileFormatFactorySpannerChangeStreamsToPubSub.getChainIdFromNftIndexV3Id(
            "flow-m.A.097bafa4e0b48eef.CharityNFT.000000000000000000000000000000000000000000000000000000000000000000000000000004"));

    Assert.assertEquals(
        "solana-101",
        FileFormatFactorySpannerChangeStreamsToPubSub.getChainIdFromNftIndexV3Id(
            "solana-101.1115Tg5qkcjvGmoYSxtNRZXNaQcSjZ1HDsG8Px6cQxf.000000000000000000000000000000000000000000000000000000000000000000000000000000"));
  }

  @Test
  public void test_getIdsFromModsArray() {
    JsonObject jsonObj = JsonParser.parseString(json).getAsJsonObject();
    Collection<String> ids =
        FileFormatFactorySpannerChangeStreamsToPubSub.getIdsFromModsArray(
            jsonObj.get("mods").getAsJsonArray());

    Assert.assertEquals(2, ids.size());
    Assert.assertTrue(
        ids.contains(
            "0xc0af40e8932ad93ed3237dd0c3fe1efd698c1efe_0x54bc3fc3977785922336084315318fa33871234_evm-1_27"));
    Assert.assertTrue(
        ids.contains(
            "0xc0af40e8932ad93ed3237dd0c3fe1efd698c1efe_0x54bc3fc3977785922336084315318fa3387eec17_evm-es_27"));
  }

  static final String json =
      "{\r\n\t    \"mods\":\r\n\t    [\r\n\t        {\r\n\t            \"keysJson\": \"{\\\"id\\\":\\\"0xc0af40e8932ad93ed3237dd0c3fe1efd698c1efe_0x54bc3fc3977785922336084315318fa3387eec17_evm-es_27\\\"}\",\r\n\t            \"oldValuesJson\": \"{}\",\r\n\t            \"newValuesJson\": \"{}\"\r\n\t        },\r\n\t        {\r\n\t            \"keysJson\": \"{\\\"id\\\":\\\"0xc0af40e8932ad93ed3237dd0c3fe1efd698c1efe_0x54bc3fc3977785922336084315318fa33871234_evm-1_27\\\"}\",\r\n\t            \"oldValuesJson\": \"{}\",\r\n\t            \"newValuesJson\": \"{}\"\r\n\t        }\r\n\t    ]\r\n\t}";
}
