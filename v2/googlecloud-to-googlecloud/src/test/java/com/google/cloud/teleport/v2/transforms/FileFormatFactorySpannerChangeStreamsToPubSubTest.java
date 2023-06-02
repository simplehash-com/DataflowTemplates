package com.google.cloud.teleport.v2.transforms;

import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class FileFormatFactorySpannerChangeStreamsToPubSubTest {

    @Test
    public void test_getChainIdFromNftId() {
        Assert.assertEquals("evm-1", FileFormatFactorySpannerChangeStreamsToPubSub
                .getChainIdFromNftId("0x06012c8cf97bead5deae237070f9587f8e7a266d_evm-1_1609764"));

        Assert.assertEquals("solana-101", FileFormatFactorySpannerChangeStreamsToPubSub
                .getChainIdFromNftId("1115Tg5qkcjvGmoYSxtNRZXNaQcSjZ1HDsG8Px6cQxf_solana-101_0"));

        Assert.assertEquals("flow-m", FileFormatFactorySpannerChangeStreamsToPubSub
                .getChainIdFromNftId("A.097bafa4e0b48eef.CharityNFT_flow-m_0"));
    }

    @Test
    public void test_getChainIdFromModelsOwner() {
        Assert.assertEquals("evm-pg", FileFormatFactorySpannerChangeStreamsToPubSub
                .getChainIdFromModelsOwnerId(
                        "0x000000000000000000000000000000000000000a_0x07515cbe746d569619dace60da2828355bf21255_evm-pg_10"));

        Assert.assertEquals("evm-ar", FileFormatFactorySpannerChangeStreamsToPubSub
                .getChainIdFromModelsOwnerId(
                        "0x0000000000000000000000000000000011111111_0xfb39b00439950e1062420b78b12d6cc63377b364_evm-ar_104 "));

    }

    @Test
    public void test_getIdsFromModsArray() {
        JsonObject jsonObj = JsonParser.parseString(json).getAsJsonObject();
        Collection<String> ids = FileFormatFactorySpannerChangeStreamsToPubSub
                .getIdsFromModsArray(jsonObj.get("mods").getAsJsonArray());

        Assert.assertEquals(2, ids.size());
        Assert.assertTrue(ids.contains(
                "0xc0af40e8932ad93ed3237dd0c3fe1efd698c1efe_0x54bc3fc3977785922336084315318fa33871234_evm-1_27"));
        Assert.assertTrue(ids.contains(
                "0xc0af40e8932ad93ed3237dd0c3fe1efd698c1efe_0x54bc3fc3977785922336084315318fa3387eec17_evm-es_27"));
    }

    static final String json = "{\r\n\t    \"mods\":\r\n\t    [\r\n\t        {\r\n\t            \"keysJson\": \"{\\\"id\\\":\\\"0xc0af40e8932ad93ed3237dd0c3fe1efd698c1efe_0x54bc3fc3977785922336084315318fa3387eec17_evm-es_27\\\"}\",\r\n\t            \"oldValuesJson\": \"{}\",\r\n\t            \"newValuesJson\": \"{}\"\r\n\t        },\r\n\t        {\r\n\t            \"keysJson\": \"{\\\"id\\\":\\\"0xc0af40e8932ad93ed3237dd0c3fe1efd698c1efe_0x54bc3fc3977785922336084315318fa33871234_evm-1_27\\\"}\",\r\n\t            \"oldValuesJson\": \"{}\",\r\n\t            \"newValuesJson\": \"{}\"\r\n\t        }\r\n\t    ]\r\n\t}";

}