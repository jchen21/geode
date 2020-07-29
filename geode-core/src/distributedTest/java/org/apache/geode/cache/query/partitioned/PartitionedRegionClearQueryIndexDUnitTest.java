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
package org.apache.geode.cache.query.partitioned;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.stream.IntStream;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

public class PartitionedRegionClearQueryIndexDUnitTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  private static final int NUM_ENTRIES = 1000;

  @Test
  public void testIndex() {
    VM vm1 = VM.getVM(0);
    VM vm2 = VM.getVM(1);
    String regionName = "testRegion";

    vm2.invoke(() -> {
      cacheRule.createCache();
      Cache cache = cacheRule.getCache();
      createRegion(cache, regionName);
    });

    vm1.invoke(() -> {
      cacheRule.createCache();
      Cache cache = cacheRule.getCache();
      Region region = createRegion(cache, regionName);
      populateRegion(region);
      createIndex(region);

//      region.clear();
//      assertIndex(region, 0);
    });

    cacheRule.createCache();
    assertIndex(cacheRule.getCache().getRegion(regionName), NUM_ENTRIES);
  }

  private Region createRegion(Cache cache, String regionName) {
    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    return cache.createRegionFactory().setPartitionAttributes(partitionAttributesFactory.create()).create(regionName);
  }

  private void populateRegion(Region region) {
    IntStream.range(0, NUM_ENTRIES).forEach(i -> region.put(i, i));
  }

  private void createIndex(Region region)
      throws IndexNameConflictException, IndexExistsException, RegionNotFoundException {
    QueryService queryService = cacheRule.getCache().getQueryService();
    queryService.createIndex("myindex", "entry.value", "/testRegion.entrySet entry");
    queryService.createKeyIndex("mykeyindex", "r.key", "/testRegion r");
  }

  private void assertIndex(Region region, int num) {
    QueryService queryService = cacheRule.getCache().getQueryService();
    assertThat(queryService.getIndexes().size()).isEqualTo(2);
    assertThat(queryService.getIndex(region, "myindex")).isNotNull();
    assertThat(queryService.getIndex(region, "mykeyindex")).isNotNull();
//    assertThat(queryService.getIndex(region, "myindex").getStatistics().getNumberOfKeys()).isEqualTo(num);
    assertThat(queryService.getIndex(region, "myindex").getStatistics().getNumberOfValues()).isEqualTo(num);
    assertThat(queryService.getIndex(region, "mykeyindex").getStatistics().getNumberOfKeys()).isEqualTo(num);
    assertThat(queryService.getIndex(region, "mykeyindex").getStatistics().getNumberOfValues()).isEqualTo(num);
  }
}
