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
package org.apache.geode.cache.client.internal;

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.LeaseExpiredException;
import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

/**
 * Tests cacheserver ssl support added. See https://svn.gemstone.com/trac/gemfire/ticket/48995 for
 * details
 */
@Category({ClientServerTest.class})
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@SuppressWarnings("serial")
public class CacheServerSSLConnectionDUnitTest extends JUnit4DistributedTestCase {

  private static final String TRUSTED_STORE = "trusted.keystore";
  private static final String CLIENT_KEY_STORE = "default.keystore";
  private static final String CLIENT_TRUST_STORE = "default.keystore";
  private static final String SERVER_KEY_STORE = "default.keystore";
  private static final String SERVER_TRUST_STORE = "default.keystore";

  private static CacheServerSSLConnectionDUnitTest instance;

  private Cache cache;
  private CacheServer cacheServer;
  private ClientCache clientCache;
  private int cacheServerPort;
  private String hostName;

  private static boolean useOldSSLSettings = true;

  public static final Logger logger = LogService.getLogger();

  @Parameters
  public static Collection<Boolean> data() {
    List<Boolean> result = new ArrayList<>();
    result.add(Boolean.TRUE);
    // result.add(Boolean.FALSE);
    return result;
  }

  public CacheServerSSLConnectionDUnitTest(Boolean useOldSSLSettings) {
    CacheServerSSLConnectionDUnitTest.useOldSSLSettings = useOldSSLSettings;
  }

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @AfterClass
  public static void postClass() {
    invokeInEveryVM(() -> {
      if (instance.cache != null) {
        instance.cache.close();
      }
      instance = null;
    });
    if (instance.cache != null) {
      instance.cache.close();
    }
    instance = null;
  }

  @Before
  public void setUp() {
    disconnectAllFromDS();
    instance = this;
    invokeInEveryVM(() -> instance = new CacheServerSSLConnectionDUnitTest(useOldSSLSettings));
  }

  @After
  public void tearDown() {
    VM serverVM = getVM(1);
    VM clientVM = getVM(2);

    clientVM.invoke(() -> closeClientCacheTask());
    serverVM.invoke(() -> closeCacheTask());
  }

  private Cache createCache(Properties props) throws Exception {
    props.setProperty(MCAST_PORT, "0");
    cache = new CacheFactory(props).create();
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  private int createServer() throws IOException {
    cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    hostName = cacheServer.getHostnameForClients();
    cacheServerPort = cacheServer.getPort();
    return cacheServerPort;
  }

  private int getCacheServerPort() {
    return cacheServerPort;
  }

  private String getCacheServerHost() {
    return hostName;
  }

  private void setUpServerVM(final boolean cacheServerSslenabled, int optionalLocatorPort)
      throws Exception {
    System.setProperty("javax.net.debug", "ssl,handshake");

    Properties gemFireProps = new Properties();
    if (optionalLocatorPort > 0) {
      gemFireProps.setProperty("locators", "localhost[" + optionalLocatorPort + "]");
    }

    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";
    boolean cacheServerSslRequireAuth = true;
    if (!useOldSSLSettings) {
      getNewSSLSettings(gemFireProps, cacheServerSslprotocols, cacheServerSslciphers,
          cacheServerSslRequireAuth);
    } else {
      gemFireProps.setProperty(CLUSTER_SSL_ENABLED, String.valueOf(cacheServerSslenabled));
      gemFireProps.setProperty(CLUSTER_SSL_PROTOCOLS, cacheServerSslprotocols);
      gemFireProps.setProperty(CLUSTER_SSL_CIPHERS, cacheServerSslciphers);
      gemFireProps.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION,
          String.valueOf(cacheServerSslRequireAuth));

      String keyStore =
          createTempFileFromResource(CacheServerSSLConnectionDUnitTest.class, SERVER_KEY_STORE)
              .getAbsolutePath();
      String trustStore =
          createTempFileFromResource(CacheServerSSLConnectionDUnitTest.class,
              SERVER_TRUST_STORE).getAbsolutePath();
      gemFireProps.setProperty(CLUSTER_SSL_KEYSTORE_TYPE, "jks");
      gemFireProps.setProperty(CLUSTER_SSL_KEYSTORE, keyStore);
      gemFireProps.setProperty(CLUSTER_SSL_KEYSTORE_PASSWORD, "password");
      gemFireProps.setProperty(CLUSTER_SSL_TRUSTSTORE, trustStore);
      gemFireProps.setProperty(CLUSTER_SSL_TRUSTSTORE_PASSWORD, "password");
    }
    StringWriter sw = new StringWriter();
    PrintWriter writer = new PrintWriter(sw);
    gemFireProps.list(writer);
    System.out.println("Starting cacheserver ds with following properties \n" + sw);
    createCache(gemFireProps);

    RegionFactory factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region r = factory.create("serverRegion");
    r.put("serverkey", "servervalue");
    DistributedLockService.create("serviceName", cache.getDistributedSystem());
  }

  private void getNewSSLSettings(Properties gemFireProps, String cacheServerSslprotocols,
      String cacheServerSslciphers, boolean cacheServerSslRequireAuth) {
    gemFireProps.setProperty(SSL_ENABLED_COMPONENTS,
        SecurableCommunicationChannel.CLUSTER + "," + SecurableCommunicationChannel.SERVER);
    gemFireProps.setProperty(SSL_PROTOCOLS, cacheServerSslprotocols);
    gemFireProps.setProperty(SSL_CIPHERS, cacheServerSslciphers);
    gemFireProps.setProperty(SSL_REQUIRE_AUTHENTICATION, String.valueOf(cacheServerSslRequireAuth));

    String keyStore =
        createTempFileFromResource(CacheServerSSLConnectionDUnitTest.class, SERVER_KEY_STORE)
            .getAbsolutePath();
    String trustStore =
        createTempFileFromResource(CacheServerSSLConnectionDUnitTest.class, SERVER_TRUST_STORE)
            .getAbsolutePath();
    gemFireProps.setProperty(SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.setProperty(SSL_KEYSTORE, keyStore);
    gemFireProps.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    gemFireProps.setProperty(SSL_TRUSTSTORE, trustStore);
    gemFireProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
  }

  private void getOldSSLSettings(Properties gemFireProps, String cacheServerSslprotocols,
      String cacheServerSslciphers, boolean cacheServerSslRequireAuth,
      final boolean cacheServerSslenabled) {
    gemFireProps.setProperty(CLUSTER_SSL_ENABLED, String.valueOf(cacheServerSslenabled));
    gemFireProps.setProperty(CLUSTER_SSL_PROTOCOLS, cacheServerSslprotocols);
    gemFireProps.setProperty(CLUSTER_SSL_CIPHERS, cacheServerSslciphers);
    gemFireProps.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION,
        String.valueOf(cacheServerSslRequireAuth));

    String keyStore =
        createTempFileFromResource(CacheServerSSLConnectionDUnitTest.class, SERVER_KEY_STORE)
            .getAbsolutePath();
    String trustStore =
        createTempFileFromResource(CacheServerSSLConnectionDUnitTest.class,
            SERVER_TRUST_STORE).getAbsolutePath();
    gemFireProps.setProperty(CLUSTER_SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.setProperty(CLUSTER_SSL_KEYSTORE, keyStore);
    gemFireProps.setProperty(CLUSTER_SSL_KEYSTORE_PASSWORD, "password");
    gemFireProps.setProperty(CLUSTER_SSL_TRUSTSTORE, trustStore);
    gemFireProps.setProperty(CLUSTER_SSL_TRUSTSTORE_PASSWORD, "password");
  }

  private void setUpClientVM(String host, int port, boolean cacheServerSslenabled,
      boolean cacheServerSslRequireAuth, String keyStore, String trustStore, boolean subscription,
      boolean clientHasTrustedKeystore) {
    System.setProperty("javax.net.debug", "ssl,handshake");
    Properties gemFireProps = new Properties();

    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";

    String keyStorePath =
        createTempFileFromResource(CacheServerSSLConnectionDUnitTest.class, keyStore)
            .getAbsolutePath();
    String trustStorePath =
        createTempFileFromResource(CacheServerSSLConnectionDUnitTest.class, trustStore)
            .getAbsolutePath();

    if (cacheServerSslenabled) {
      if (useOldSSLSettings) {
        gemFireProps.setProperty(SERVER_SSL_ENABLED, String.valueOf(cacheServerSslenabled));
        gemFireProps.setProperty(SERVER_SSL_PROTOCOLS, cacheServerSslprotocols);
        gemFireProps.setProperty(SERVER_SSL_CIPHERS, cacheServerSslciphers);
        gemFireProps.setProperty(SERVER_SSL_REQUIRE_AUTHENTICATION,
            String.valueOf(cacheServerSslRequireAuth));
        if (clientHasTrustedKeystore) {
          gemFireProps.setProperty(SERVER_SSL_KEYSTORE_TYPE, "jks");
          gemFireProps.setProperty(SERVER_SSL_KEYSTORE, keyStorePath);
          gemFireProps.setProperty(SERVER_SSL_KEYSTORE_PASSWORD, "password");
          gemFireProps.setProperty(SERVER_SSL_TRUSTSTORE, trustStorePath);
          gemFireProps.setProperty(SERVER_SSL_TRUSTSTORE_PASSWORD, "password");
        } else {
          gemFireProps.setProperty(SERVER_SSL_KEYSTORE_TYPE, "jks");
          gemFireProps.setProperty(SERVER_SSL_KEYSTORE, "");
          gemFireProps.setProperty(SERVER_SSL_KEYSTORE_PASSWORD, "password");
          gemFireProps.setProperty(SERVER_SSL_TRUSTSTORE, trustStorePath);
          gemFireProps.setProperty(SERVER_SSL_TRUSTSTORE_PASSWORD, "password");
        }
      } else {
        gemFireProps.setProperty(SSL_ENABLED_COMPONENTS, "server");
        gemFireProps.setProperty(SSL_CIPHERS, cacheServerSslciphers);
        gemFireProps.setProperty(SSL_PROTOCOLS, cacheServerSslprotocols);
        gemFireProps
            .setProperty(SSL_REQUIRE_AUTHENTICATION, String.valueOf(cacheServerSslRequireAuth));
        if (clientHasTrustedKeystore) {
          gemFireProps.setProperty(SSL_KEYSTORE_TYPE, "jks");
          gemFireProps.setProperty(SSL_KEYSTORE, keyStorePath);
          gemFireProps.setProperty(SSL_KEYSTORE_PASSWORD, "password");
          gemFireProps.setProperty(SSL_TRUSTSTORE, trustStorePath);
          gemFireProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
        } else {
          gemFireProps.setProperty(SSL_KEYSTORE_TYPE, "jks");
          gemFireProps.setProperty(SSL_KEYSTORE, "");
          gemFireProps.setProperty(SSL_KEYSTORE_PASSWORD, "password");
          gemFireProps.setProperty(SSL_TRUSTSTORE, trustStorePath);
          gemFireProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
        }
      }
    }

    StringWriter sw = new StringWriter();
    PrintWriter writer = new PrintWriter(sw);
    gemFireProps.list(writer);
    System.out.println("Starting client ds with following properties \n" + sw.getBuffer());

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory(gemFireProps);
    clientCacheFactory.setPoolSubscriptionEnabled(subscription).addPoolServer(host, port);
    clientCacheFactory.setPoolRetryAttempts(5);
    clientCache = clientCacheFactory.create();

    ClientRegionFactory<String, String> regionFactory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
    Region<String, String> region = regionFactory.create("serverRegion");
    assertNotNull(region);
  }

  private void doClientRegionTest() {
    // try {
    // for (int i = 0; i < 100; i++) {
    //
    // boolean isLocked =
    // DistributedLockService.getServiceNamed("serviceName").lock("lock1", 5_000, -1);
    // if (isLocked) {
    // logger.info("client acquired the lock.");
    // Thread.sleep(10000);
    // break;
    // }
    // logger.info("client retry");
    // Thread.sleep(100);
    // }
    // } catch (Exception e) {
    // e.printStackTrace();
    // } finally {
    // logger.info("client unlock");
    // DistributedLockService.getServiceNamed("serviceName").unlock("lock1");
    // }
    Region<String, String> region = clientCache.getRegion("serverRegion");
    assertEquals("servervalue", region.get("serverkey"));
    region.put("clientkey", "clientvalue");
    assertEquals("clientvalue", region.get("clientkey"));
  }

  private void doServerRegionTest() throws ExecutionException, InterruptedException {
    ExecutorService executorService = Executors.newCachedThreadPool();
    DistributedSystem.setThreadsSocketPolicy(false);
    Future future1 = executorService.submit(() -> {
      for (int req = 0; req < 10; req++) {
        try {
          int i;
          for (i = 0; i < 100; i++) {

            boolean isLocked =
                DistributedLockService.getServiceNamed("serviceName").lock("lock1", 1_000, 5_000);
            if (isLocked) {
              logger.info("server acquired the lock1.");
              Thread.sleep(1000);
              break;
            } else {
              logger.info("server retry");
              try {
                Thread.sleep(10000);
              } catch (Exception e) {
                logger.error(e);
              }
            }
          }

          if (i == 100) {
            logger.error("server failed to acquire the lock1");
          }
        } catch (Exception e) {
          logger.error(e);
        } finally {
          logger.info("server unlock lock1");
          try {
            DistributedLockService.getServiceNamed("serviceName").unlock("lock1");
          } catch (LeaseExpiredException exception) {
            logger.info("Caught LeaseExpiredException");
          }
          DistributedSystem.releaseThreadsSockets();
        }

        // sleep in between lock requests
        try {
          Thread.sleep(1000);
        } catch (Exception e) {
          logger.error(e);
        }
      }
    });

    Future future2 = executorService.submit(() -> {
      try {
        int i;
        for (i = 0; i < 100; i++) {

          boolean isLocked =
              DistributedLockService.getServiceNamed("serviceName").lock("lock2", 1_000, 5_000);
          if (isLocked) {
            logger.info("server acquired the lock2.");
            Thread.sleep(1000);
            break;
          } else {
            logger.info("server retry");
            try {
              Thread.sleep(10000);
            } catch (Exception e) {
              logger.error(e);
            }
          }
        }

        if (i == 100) {
          logger.error("server failed to acquire the lock2");
        }
      } catch (Exception e) {
        logger.error(e);
      } finally {
        logger.info("server unlock lock2");
        try {
          DistributedLockService.getServiceNamed("serviceName").unlock("lock2");
        } catch (LeaseExpiredException exception) {
          logger.info("Caught LeaseExpiredException");
        }
        DistributedSystem.releaseThreadsSockets();
      }
    });

    future1.get();
    future2.get();
    executorService.awaitTermination(60, TimeUnit.SECONDS);
    executorService.shutdown();

    Region<String, String> region = cache.getRegion("serverRegion");
    // assertEquals("servervalue", region.get("serverkey"));
    // assertEquals("clientvalue", region.get("clientkey"));
  }


  private static void setUpServerVMTask(boolean cacheServerSslenabled, int optionalLocatorPort)
      throws Exception {
    instance.setUpServerVM(cacheServerSslenabled, optionalLocatorPort);
  }

  private static int createServerTask() throws Exception {
    return instance.createServer();
  }

  private static void setUpClientVMTask(String host, int port, boolean cacheServerSslenabled,
      boolean cacheServerSslRequireAuth, String keyStore, String trustStore,
      boolean clientHasTrustedKeystore) {
    instance.setUpClientVM(host, port, cacheServerSslenabled, cacheServerSslRequireAuth, keyStore,
        trustStore, true, clientHasTrustedKeystore);
  }

  private static void setUpClientVMTaskNoSubscription(String host, int port,
      boolean cacheServerSslenabled, boolean cacheServerSslRequireAuth, String keyStore,
      String trustStore) {
    instance.setUpClientVM(host, port, cacheServerSslenabled, cacheServerSslRequireAuth, keyStore,
        trustStore, false, true);
  }

  private static void doClientRegionTestTask() {
    instance.doClientRegionTest();
  }

  private static void verifyServerDoesNotReceiveClientUpdate() {
    instance.doVerifyServerDoesNotReceiveClientUpdate();
  }

  private void doVerifyServerDoesNotReceiveClientUpdate() {
    Region<String, String> region = cache.getRegion("serverRegion");
    assertFalse(region.containsKey("clientkey"));
  }

  private static void doServerRegionTestTask() throws ExecutionException, InterruptedException {
    // for (int i = 0; i < 10; i++) {
    instance.doServerRegionTest();
    // }
  }

  private static Object[] getCacheServerEndPointTask() { // TODO: avoid Object[]
    Object[] array = new Object[2];
    array[0] = instance.getCacheServerHost();
    array[1] = instance.getCacheServerPort();
    return array;
  }

  private static void closeCacheTask() {
    if (instance != null && instance.cache != null) {
      instance.cache.close();
    }
  }

  private static void closeClientCacheTask() {
    if (instance != null && instance.clientCache != null) {
      instance.clientCache.close();
    }
  }

  @Test
  public void testCacheServerSSL() throws Exception {
    VM serverVM = getVM(0);
    // VM clientVM = getVM(2);
    VM serverVM2 = getVM(1);
    VM serverVM3 = getVM(2);
    VM serverVM4 = getVM(3);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;

    Properties locatorProps = new Properties();
    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";
    boolean cacheServerSslRequireAuth = true;
    if (!useOldSSLSettings) {
      getNewSSLSettings(locatorProps, cacheServerSslprotocols, cacheServerSslciphers,
          cacheServerSslRequireAuth);
    } else {
      getOldSSLSettings(locatorProps, cacheServerSslprotocols, cacheServerSslciphers,
          cacheServerSslRequireAuth, cacheServerSslenabled);
    }
    Locator locator = Locator.startLocatorAndDS(0, new File(""), locatorProps);
    int locatorPort = locator.getPort();
    try {
      serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled, locatorPort));
      int port = serverVM.invoke(() -> createServerTask());
      serverVM2.invoke(() -> setUpServerVMTask(cacheServerSslenabled, locatorPort));
      serverVM2.invoke(() -> createServerTask());
      serverVM3.invoke(() -> setUpServerVMTask(cacheServerSslenabled, locatorPort));
      serverVM3.invoke(() -> createServerTask());
      serverVM4.invoke(() -> setUpServerVMTask(cacheServerSslenabled, locatorPort));
      serverVM4.invoke(() -> createServerTask());

      String hostName = getHostName();

      // clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled,
      // cacheClientSslRequireAuth, CLIENT_KEY_STORE, CLIENT_TRUST_STORE, true));



      // AsyncInvocation clientAsync = clientVM.invokeAsync(() -> doClientRegionTestTask());
      AsyncInvocation serverAsync = serverVM.invokeAsync(() -> doServerRegionTestTask());
      AsyncInvocation serverAsync2 = serverVM2.invokeAsync(() -> doServerRegionTestTask());
      AsyncInvocation serverAsync3 = serverVM3.invokeAsync(() -> doServerRegionTestTask());
      AsyncInvocation serverAsync4 = serverVM4.invokeAsync(() -> doServerRegionTestTask());

      // clientAsync.get();
      serverAsync.get();
      serverAsync2.get();
      serverAsync3.get();
      serverAsync4.get();
    } finally {
      locator.stop();
    }
  }

  /**
   * GEODE-2898: A non-responsive SSL client can block a server's "acceptor" thread
   *
   * <p>
   * Start a server and then connect to it without completing the SSL handshake
   *
   * <p>
   * Attempt to connect to the server using a real SSL client, demonstrating that the server is not
   * blocked and can process the new connection request.
   */

  public void clientSlowToHandshakeDoesNotBlockServer() throws Throwable {
    VM serverVM = getVM(1);
    VM clientVM = getVM(2);
    VM slowClientVM = getVM(3);

    getBlackboard().initBlackboard();

    // a plain-text socket is used to connect to an ssl server & the handshake
    // is never performed. The server will log this exception & it should be ignored
    addIgnoredException(SSLHandshakeException.class);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled, 0));
    int port = serverVM.invoke(() -> createServerTask());

    String hostName = getHostName();

    AsyncInvocation slowAsync = slowClientVM.invokeAsync(() -> connectToServer(hostName, port));
    try {
      getBlackboard().waitForGate("serverIsBlocked", 60, TimeUnit.SECONDS);

      clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled,
          cacheClientSslRequireAuth, CLIENT_KEY_STORE, CLIENT_TRUST_STORE, true));
      clientVM.invoke(() -> doClientRegionTestTask());
      serverVM.invoke(() -> doServerRegionTestTask());

    } finally {
      getBlackboard().signalGate("testIsCompleted");
      slowAsync.await();
    }
  }

  private void connectToServer(String hostName, int port) throws Exception {
    Socket sock = new Socket();
    sock.connect(new InetSocketAddress(hostName, port));
    try {
      getBlackboard().signalGate("serverIsBlocked");
      getBlackboard().waitForGate("testIsCompleted", 60, TimeUnit.SECONDS);
    } finally {
      sock.close();
    }
  }


  public void testNonSSLClient() {
    VM serverVM = getVM(1);
    VM clientVM = getVM(2);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = false;
    boolean cacheClientSslRequireAuth = true;

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled, 0));
    serverVM.invoke(() -> createServerTask());

    Object array[] = serverVM.invoke(() -> getCacheServerEndPointTask());
    String hostName = (String) array[0];
    int port = (Integer) array[1];

    try (IgnoredException i1 = addIgnoredException(SSLException.class);
        IgnoredException i2 = addIgnoredException(IOException.class)) {
      clientVM.invoke(() -> setUpClientVMTaskNoSubscription(hostName, port, cacheClientSslenabled,
          cacheClientSslRequireAuth, TRUSTED_STORE, TRUSTED_STORE));
      clientVM.invoke(() -> doClientRegionTestTask());
      serverVM.invoke(() -> doServerRegionTestTask());
      fail("Test should fail as non-ssl client is trying to connect to ssl configured server");

    } catch (Exception rmiException) {
      assertThat(rmiException).hasRootCauseInstanceOf(AuthenticationRequiredException.class);
    }
  }


  public void testSSLClientWithNoAuth() {
    VM serverVM = getVM(1);
    VM clientVM = getVM(2);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = false;

    addIgnoredException("SSLHandshakeException");
    addIgnoredException("ValidatorException");

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled, 0));
    serverVM.invoke(() -> createServerTask());

    Object array[] = serverVM.invoke(() -> getCacheServerEndPointTask());
    String hostName = (String) array[0];
    int port = (Integer) array[1];

    clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled,
        cacheClientSslRequireAuth, CLIENT_KEY_STORE, CLIENT_TRUST_STORE, true));
    clientVM.invoke(() -> doClientRegionTestTask());
    serverVM.invoke(() -> doServerRegionTestTask());
  }


  public void untrustedClientIsRejected() {
    VM serverVM = getVM(1);
    VM clientVM = getVM(2);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = false;

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled, 0));
    serverVM.invoke(() -> createServerTask());

    Object array[] = serverVM.invoke(() -> getCacheServerEndPointTask());
    String hostName = (String) array[0];
    int port = (Integer) array[1];

    addIgnoredException("SSLHandshakeException");

    clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled,
        cacheClientSslRequireAuth, "default.keystore", CLIENT_TRUST_STORE, false));

    try {
      clientVM.invoke(() -> doClientRegionTestTask());
      fail("client should not have been able to execute a cache operation");
    } catch (RMIException e) {
      assertThat(e).hasRootCauseInstanceOf(NoAvailableServersException.class);
    }
    serverVM.invoke(() -> verifyServerDoesNotReceiveClientUpdate());
  }


  public void testSSLClientWithNonSSLServer() {
    VM serverVM = getVM(1);
    VM clientVM = getVM(2);

    boolean cacheServerSslenabled = false;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled, 0));
    serverVM.invoke(() -> createServerTask());

    Object array[] = serverVM.invoke(() -> getCacheServerEndPointTask());
    String hostName = (String) array[0];
    int port = (Integer) array[1];

    try (IgnoredException i = addIgnoredException(SSLHandshakeException.class)) {
      clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled,
          cacheClientSslRequireAuth, TRUSTED_STORE, TRUSTED_STORE, true));
      clientVM.invoke(() -> doClientRegionTestTask());
      serverVM.invoke(() -> doServerRegionTestTask());
      fail(
          "Test should fail as ssl client with ssl enabled is trying to connect to server with ssl disabled");

    } catch (Exception e) {
      // ignore
      assertThat(e).hasRootCauseInstanceOf(NoAvailableServersException.class);
    }
  }
}
