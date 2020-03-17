/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.data.PortfolioData;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

import static org.assertj.core.api.Assertions.assertThat;

public class PartitionedRegionClearPersistentOverflowDUnitTest implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(5);

  private MemberVM locator, server1, server2, server3;

  private ClientVM client;

  private String regionName = "testRegion";

  private int numEntries = 100;

  @Before
  public void setup() throws Exception {
    locator = clusterStartupRule.startLocatorVM(0, 0);
    server1 = clusterStartupRule.startServerVM(1, locator.getPort());
    server2 = clusterStartupRule.startServerVM(2, locator.getPort());
    server3 = clusterStartupRule.startServerVM(3, locator.getPort());
    client = clusterStartupRule.startClientVM(4, c -> c.withLocatorConnection(locator.getPort()));
  }

  private File getDiskDir() {
    int vmNum = VM.getVMId();
    File dir = new File("diskDir", "disk" + vmNum).getAbsoluteFile();
    dir.mkdirs();
    return dir;
  }

  /**
   * Return a set of disk directories for persistence tests. These directories will be automatically
   * cleaned up on test case closure.
   */
  private File[] getDiskDirs() {
    return new File[] {getDiskDir()};
  }

  private void createRegionAndDiskOnServer(MemberVM server, RegionShortcut type, int numBuckets,
                                    int redundancy, EvictionAlgorithm evictionAlgorithm) {
    server.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();

      cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");

      EvictionAttributesImpl evicAttr =
          new EvictionAttributesImpl().setAction(EvictionAction.OVERFLOW_TO_DISK);
      evicAttr.setAlgorithm(evictionAlgorithm).setMaximum(1);

      cache.createRegionFactory(type)
          .setPartitionAttributes(
              new PartitionAttributesFactory().setTotalNumBuckets(numBuckets)
                  .setRedundantCopies(redundancy).create()).setEvictionAttributes(evicAttr)
          .create(regionName);
    });
  }

  private void createRegionOnClient(ClientRegionShortcut type) {
    client.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      clientCache.createClientRegionFactory(type).create(regionName);
    });
  }

  private void createRegionAndDiskInCluster(RegionShortcut type, int numBuckets, int redundancy) {
    createRegionAndDiskOnServer(server1, type, numBuckets, redundancy, EvictionAlgorithm.LRU_ENTRY);
    createRegionAndDiskOnServer(server2, type, numBuckets, redundancy, EvictionAlgorithm.LRU_ENTRY);
    createRegionAndDiskOnServer(server3, type, numBuckets, redundancy, EvictionAlgorithm.LRU_ENTRY);
    createRegionOnClient(ClientRegionShortcut.CACHING_PROXY);
  }

  private void populateRegion() {
    client.invoke(() -> {
      Region clientRegion = ClusterStartupRule.getClientCache().getRegion("/" + regionName);
      Map<String, String> entries = new HashMap<>();
      IntStream.range(0, numEntries).forEach(i -> entries.put("key-" + i, "value-" + i));
      entries.entrySet().forEach(e -> {
        clientRegion.put(e.getKey(), e.getValue());
      });
    });
  }

  private void assertRegionSizeOnServer(int size) {
    server1.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion(regionName);
      assertThat(region.size()).isEqualTo(size);
    });
  }

  private void assertRegionAttributesOnServer(int numBuckets, boolean persistent, int redundancy) {
    server1.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion(regionName);
      assertThat(region.getAttributes().getPartitionAttributes().getTotalNumBuckets())
          .isEqualTo(numBuckets);
      assertThat(region.getAttributes().getDataPolicy().withPersistence()).isEqualTo(persistent);
      assertThat(region.getAttributes().getPartitionAttributes().getRedundantCopies())
          .isEqualTo(redundancy);
    });
  }


  private void doRegionClear(String methodName, boolean isClient) {
    if (isClient) {
      client.invoke(() -> {
        Region clientRegion = ClusterStartupRule.getClientCache().getRegion(regionName);
        long startTime = System.currentTimeMillis();
        clientRegion.clear();
        long endTime = System.currentTimeMillis();
        System.out.println(
            "Partitioned region with " + numEntries + " entries takes " + (endTime - startTime)
                + " milliseconds to clear. methodName=" + methodName + " isClient=" + isClient);
        assertThat(clientRegion.size()).isEqualTo(0);
      });
    } else {
      server1.invoke(() -> {
        Region region = ClusterStartupRule.getCache().getRegion(regionName);
        long startTime = System.currentTimeMillis();
        region.clear();
        long endTime = System.currentTimeMillis();
        System.out.println(
            "Partitioned region with " + numEntries + " entries takes " + (endTime - startTime)
                + " milliseconds to clear. methodName=" + methodName + " isClient=" + isClient);
      });
    }
  }

  @Test
  public void test() {
    createRegionAndDiskInCluster(RegionShortcut.PARTITION_OVERFLOW, 113, 2);
    populateRegion();
    assertRegionSizeOnServer(numEntries);
    doRegionClear("test", false);
    assertRegionSizeOnServer(0);
//    server1.invoke(() -> {
//      Cache cache = ClusterStartupRule.getCache();
//
//      cache.createDiskStoreFactory().setDiskDirs(getDiskDirs()).create("disk");
//
//      EvictionAttributesImpl evicAttr =
//          new EvictionAttributesImpl().setAction(EvictionAction.OVERFLOW_TO_DISK);
//      evicAttr.setAlgorithm(EvictionAlgorithm.LRU_ENTRY).setMaximum(1);
//
//      cache.createRegionFactory(RegionShortcut.PARTITION_OVERFLOW)
//          .setPartitionAttributes(
//              new PartitionAttributesFactory()
//                  .setRedundantCopies(2).create()).setEvictionAttributes(evicAttr)
//          .create(regionName);
//    });
  }

}
