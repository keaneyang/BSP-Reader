package com.bloom.proc;

import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import com.bloom.event.Event;
import com.bloom.security.Password;
import com.bloom.source.lib.intf.BaseParser;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.uuid.UUID;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class HTTPParser_1_0
  extends BaseParser
{
  private Server server;
  Map<String, Object> propertyMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
  private static final String SOURCE_UUID = "sourceuuid";
  private Logger logger = Logger.getLogger(HTTPParser_1_0.class);
  int delay = 100;
  int backLog = 20;
  private String keyStorePassword;
  private String keyStore;
  private static final String keyManagerAlgorithm = "SunX509";
  private static final String trustManagerAlgorithm = "SunX509";
  private static final String sslContextAlgorithm = "SSL";
  private boolean isSSLEnabled;
  UUID srcUUID;
  Property property;
  SourceProcess sourceProcess;
  private static String SOURCE_PROCESS = "sourceProcess";
  private BlockingQueue<Event> syncQueue = new ArrayBlockingQueue(100000);
  AsyncSender sender;
  
  class AsyncSender
    extends Thread
  {
    volatile boolean running = true;
    int count = 0;
    int lastCount = 0;
    int rCount = 0;
    int lastRCount = 0;
    long stime = -1L;
    long ttime = 0L;
    long tptime = 0L;
    
    AsyncSender() {}
    
    public void run()
    {
      while (this.running)
      {
        if (this.stime == -1L) {
          this.stime = System.nanoTime();
        }
        try
        {
          long pptime = System.nanoTime();
          Event event = (Event)HTTPParser_1_0.this.syncQueue.poll(100L, TimeUnit.MILLISECONDS);
          long sptime = System.nanoTime();
          if (event != null)
          {
            HTTPParser_1_0.this.sourceProcess.send(event, 0, null);
            this.count += 1;
            this.rCount += 1;
          }
          if (HTTPParser_1_0.this.logger.isDebugEnabled())
          {
            long etime = System.nanoTime();
            this.ttime += etime - sptime;
            this.tptime += sptime - pptime;
            double diff = (etime - this.stime) / 1000000.0D;
            if (diff > 2000.0D)
            {
              double tdiff = this.ttime / 1000000.0D;
              double pdiff = this.tptime / 1000000.0D;
              int cdiff = this.count - this.lastCount;
              int rcdiff = this.rCount - this.lastRCount;
              double rate = cdiff == 0 ? 0.0D : tdiff / cdiff;
              double rrate = rcdiff == 0 ? 0.0D : tdiff / rcdiff;
              double prate = rcdiff == 0 ? 0.0D : pdiff / rcdiff;
              if (HTTPParser_1_0.this.logger.isDebugEnabled()) {
                HTTPParser_1_0.this.logger.debug("Processed " + cdiff + "/" + this.count + " events " + rcdiff + "/" + this.rCount + " requests: ave time " + rate + " ms/e " + rrate + " ms/r + polling " + prate + " ms/r" + " - Queue size: " + HTTPParser_1_0.this.syncQueue.size());
              }
              this.stime = etime;
              this.lastCount = this.count;
              this.lastRCount = this.rCount;
              this.ttime = 0L;
              this.tptime = 0L;
            }
          }
        }
        catch (Exception e) {}
      }
    }
  }
  
  public HTTPParser_1_0(Map<String, Object> prop, UUID sourceUUID)
    throws AdapterException
  {
    super(prop, sourceUUID);
    this.property = new Property(prop);
    this.srcUUID = sourceUUID;
  }
  
  public Iterator<Event> parse(Reader in)
    throws AdapterException
  {
    try
    {
      validateSSLProperties(this.property);
      String ipaddress = this.property.ipaddress;
      int portNo = this.property.portno;
      this.propertyMap.put("sourceuuid", this.srcUUID);
      this.propertyMap.putAll(this.property.getMap());
      if (this.logger.isTraceEnabled()) {
        this.logger.trace("HTTPReader is initialized with following properties \nIPAddress - [" + ipaddress + "]\n" + "Port No - [" + portNo + "]");
      }
      this.sourceProcess = ((SourceProcess)this.propertyMap.get(SOURCE_PROCESS));
      
      this.sender = new AsyncSender();
      this.sender.setName("HTTPReader-" + ipaddress + ":" + portNo + "-Event-Synchronizer");
      this.sender.start();
      this.server = new Server();
      SelectChannelConnector connector;
      SelectChannelConnector connector;
      if (this.isSSLEnabled)
      {
        if (this.logger.isTraceEnabled()) {
          this.logger.trace("SSL is enabled with following properties\nKeystore type - [" + this.property.keyStoreType + "]\n" + "keystore location - [" + this.property.keyStore + "]\n" + "Client authentication - [" + this.property.authenticateClient + "]");
        }
        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setSslContext(getSSLContext());
        sslContextFactory.setNeedClientAuth(this.property.authenticateClient);
        connector = new SslSelectChannelConnector(sslContextFactory);
      }
      else
      {
        connector = new SelectChannelConnector();
      }
      connector.setHost(ipaddress);
      connector.setPort(portNo);
      this.server.addConnector(connector);
      this.server.setHandler(new HTTPRequestHandler(this.propertyMap, this.syncQueue));
      this.server.start();
      return this;
    }
    catch (Exception e)
    {
      throw new AdapterException("Failure in initializing HTTPReader", e);
    }
  }
  
  private SSLContext getSSLContext()
    throws AdapterException
  {
    try
    {
      char[] password = null;
      FileInputStream stream = null;
      if (this.keyStore != null) {
        stream = new FileInputStream(this.keyStore);
      }
      if (this.keyStorePassword != null) {
        password = this.keyStorePassword.toCharArray();
      }
      KeyStore serverKeyStore = KeyStore.getInstance(this.property.keyStoreType);
      serverKeyStore.load(stream, password);
      KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
      kmf.init(serverKeyStore, password);
      TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
      tmf.init(serverKeyStore);
      SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
      return sslContext;
    }
    catch (KeyStoreException|NoSuchAlgorithmException|CertificateException|IOException|UnrecoverableKeyException|KeyManagementException e)
    {
      AdapterException se = new AdapterException(Error.GENERIC_EXCEPTION, e);
      throw se;
    }
  }
  
  private void validateSSLProperties(Property prop)
    throws AdapterException
  {
    String keyStoreType = prop.keyStoreType;
    if ((keyStoreType != null) && (!keyStoreType.isEmpty())) {
      this.isSSLEnabled = true;
    }
    if (this.isSSLEnabled)
    {
      this.keyStorePassword = prop.keyStorePassword;
      if (this.keyStorePassword.isEmpty()) {
        this.keyStorePassword = null;
      }
      this.keyStore = prop.keyStore;
      if (this.keyStore.isEmpty())
      {
        this.keyStore = null;
        this.logger.warn("Keystore location is empty, empty keystore will be created");
      }
    }
  }
  
  public boolean hasNext()
  {
    return false;
  }
  
  public Event next()
  {
    return null;
  }
  
  public void close()
    throws AdapterException
  {
    try
    {
      this.server.stop();
    }
    catch (Exception e)
    {
      throw new AdapterException(Error.GENERIC_EXCEPTION, e);
    }
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("HTTPReader running is closed");
    }
    this.sender.running = false;
    try
    {
      this.sender.join(100L);
    }
    catch (InterruptedException e) {}
  }
  
  public void remove() {}
  
  public static void main(String[] args)
  {
    Map<String, Object> props = new HashMap();
    props.put("ipaddress", "127.0.0.1");
    props.put("portno", Integer.valueOf(10000));
    
    props.put("keystoretype", "JKS");
    Password keystorePassword = new Password();
    keystorePassword.setPlain("bloom");
    props.put("keystorepassword", keystorePassword);
    props.put("keystore", "/Users/niranjan/server-keystore.jks");
    try
    {
      HTTPParser_1_0 x = new HTTPParser_1_0(props, null);
      x.parse(null);
      for (;;)
      {
        Thread.sleep(100L);
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}
