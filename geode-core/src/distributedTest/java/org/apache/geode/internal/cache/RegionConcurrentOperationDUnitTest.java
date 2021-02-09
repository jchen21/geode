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

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PROXY;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.pdx.WritablePdxInstance;
import org.apache.geode.test.dunit.DUnitBlackboard;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;

public class RegionConcurrentOperationDUnitTest implements Serializable {

  private static DUnitBlackboard blackboard;

  Object key = "KEY";
  String value = "VALUE";

  private static DUnitBlackboard getBlackboard() {
    if (blackboard == null) {
      blackboard = new DUnitBlackboard();
    }
    return blackboard;
  }

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  @After
  public void tearDown() {
    if (blackboard != null) {
      blackboard.initBlackboard();
    }
  }

  @Test
  public void getOnProxyRegionFromMultipleThreadsReturnsDifferentObjects() throws Exception {
    VM member1 = getVM(0);
    String regionName = getClass().getSimpleName();

    cacheRule.createCache();
    cacheRule.getCache().createRegionFactory(REPLICATE_PROXY).create(regionName);

    member1.invoke(() -> {
      cacheRule.createCache();
      cacheRule.getCache().createRegionFactory(REPLICATE)
          .setCacheLoader(new TestCacheLoader()).create(regionName);
    });

    Future get1 = executorServiceRule.submit(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      return region.get(key);
    });

    Future get2 = executorServiceRule.submit(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      getBlackboard().waitForGate("Loader", 60, TimeUnit.SECONDS);
      return region.get(key);
    });

    Object get1value = get1.get();
    Object get2value = get2.get();

    assertThat(get1value).isNotSameAs(get2value);
  }

  @Test
  public void getOnPreLoadedRegionFromMultipleThreadsReturnSameObject() throws Exception {
    VM member1 = getVM(0);
    String regionName = getClass().getSimpleName();

    cacheRule.createCache();
    cacheRule.getCache().createRegionFactory().setDataPolicy(DataPolicy.PRELOADED)
        .setScope(Scope.DISTRIBUTED_ACK).create(regionName);

    member1.invoke(() -> {
      cacheRule.createCache();
      cacheRule.getCache().createRegionFactory(REPLICATE)
          .setCacheLoader(new TestCacheLoader()).create(regionName);
    });
    assertThat(cacheRule.getCache().getRegion(regionName).size()).isEqualTo(0);

    Future get1 = executorServiceRule.submit(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      return region.get(key);
    });

    Future get2 = executorServiceRule.submit(() -> {
      Region region = cacheRule.getCache().getRegion(regionName);
      getBlackboard().waitForGate("Loader", 60, TimeUnit.SECONDS);
      return region.get(key);
    });

    Object get1value = get1.get();
    Object get2value = get2.get();

    assertThat(get1value).isSameAs(get2value);
    assertThat(cacheRule.getCache().getRegion(regionName).size()).isEqualTo(1);
  }

  @Test
  public void getOnPartitionedRegionFromMultipleThreadsReturnsDifferentPdxInstances()
      throws Exception {
    String regionName = getClass().getSimpleName();
    CacheFactory cacheFactory = new CacheFactory();
    cacheFactory.setPdxReadSerialized(true);
    cacheRule.createCache(cacheFactory);
    InternalCache cache = cacheRule.getCache();
    Region<Object, Object> region = cache.createRegionFactory(PARTITION)
        .create(regionName);

    Random random = new Random();
    final String[] CUSIPS = new String[] {
        "AAPL", "MSFT", "AMZN", "GOOGL", "FB", "JPM", "V", "JNJ", "WMT", "PG",
        "BAC", "XOM", "MA", "UNH", "VZ", "INTC", "HD", "KO", "MRK", "WFC", "PFE",
        "NVS", "TM", "CMCSA", "BA", "PEP", "CSCO", "ORCL", "C", "SAP", "HSBC",
        "ADBE", "MCD", "NKE", "NFLX", "COST", "BUD", "HON", "PYPL", "AVGO", "CRM",
        "UNP", "IBM", "LLY", "TXN", "LMT", "SBUX", "UPS", "QCOM", "CVS", "AXP",
        "MMM", "BMY", "GE"
    };

    // Keep doing this concurrency test for 30 seconds.
    long endTime = Duration.ofSeconds(30).toMillis() + System.currentTimeMillis();

    while (System.currentTimeMillis() < endTime) {
      Callable<Object> getValue = () -> {
        while (true) {
          Object tradePdx = region.get(key);
          if (tradePdx != null) {
            return tradePdx;
          }
        }
      };

      Callable<Object> updateTradeFunction = () -> {
        while (true) {
          Object tradePdx = region.get(key);
          if (tradePdx != null) {
            WritablePdxInstance tradeWritablePdx = ((PdxInstance) tradePdx).createWriter();
            int shares = random.nextInt(100);
            BigDecimal price = new BigDecimal(BigInteger.valueOf(random.nextInt(100000)), 2);
            tradeWritablePdx.setField("shares", shares);
            tradeWritablePdx.setField("price", price);
            if (tradeWritablePdx.hasField("updateTime")) {
              tradeWritablePdx.setField("updateTime", System.currentTimeMillis());
            }
            region.put(key, tradeWritablePdx);
            return tradePdx;
          }
        }
      };

      int numThreads = 10;

      Future<Object> getFuture[] = new Future[numThreads];
      for (int i = 0; i < numThreads; i++) {
        getFuture[i] = executorServiceRule.submit(getValue);
      }

      Future<Object> updateFuture[] = new Future[numThreads];
      for (int i = 0; i < numThreads; i++) {
        updateFuture[i] = executorServiceRule.submit(updateTradeFunction);
      }

      Trade trade =
          new Trade(String.valueOf(random.nextInt(100000)), CUSIPS[random.nextInt(CUSIPS.length)],
              random.nextInt(100), new BigDecimal(BigInteger.valueOf(random.nextInt(100000)), 2),
              new byte[100000], System.currentTimeMillis(), System.currentTimeMillis());
      Future<Object> putFuture = executorServiceRule.submit(() -> region.put(key, trade));

      for (int i = 0; i < numThreads; i++) {
        getFuture[i].get();
      }

      for (int i = 0; i < numThreads; i++) {
        updateFuture[i].get();
      }

      putFuture.get();

      region.destroy(key);
    }
  }

  private class Trade implements PdxSerializable {
    private final String id;
    private final String cusip;
    private final int shares;
    private final BigDecimal price;
    private final byte[] payload;
    private final long createTime;
    private final long updateTime;

    public Trade(String id, String cusip, int shares, BigDecimal price, byte[] payload,
        long createTime, long updateTime) {
      this.id = id;
      this.cusip = cusip;
      this.shares = shares;
      this.price = price;
      this.payload = payload;
      this.createTime = createTime;
      this.updateTime = updateTime;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("id", id);
      writer.writeString("cusip", cusip);
      writer.writeInt("shares", shares);
      writer.writeObject("price", price);
      writer.writeByteArray("payload", payload);
      writer.writeLong("createTime", createTime);
      writer.writeLong("updateTime", updateTime);
    }

    @Override
    public void fromData(PdxReader reader) {
      reader.readString("id");
      reader.readString("cusip");
      reader.readInt("shares");
      reader.readObject("price");
      reader.readByteArray("payload");
      reader.readLong("createTime");
      reader.readLong("updateTime");
    }
  }

  private class TestCacheLoader implements CacheLoader<Object, Object>, Serializable {

    @Override
    public synchronized Object load(LoaderHelper helper) {
      getBlackboard().signalGate("Loader");
      return value;
    }

    @Override
    public void close() {}
  }

}
