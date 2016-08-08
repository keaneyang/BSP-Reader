package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.common.exc.ConnectionException;
import com.bloom.event.Event;
import com.bloom.intf.SourceMetadataProvider;
import com.bloom.proc.events.WAEvent;
import com.bloom.runtime.compiler.TypeDefOrName;
import com.bloom.uuid.UUID;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.log4j.Logger;

@PropertyTemplate(name="DatabaseReader", type=AdapterType.source, properties={@com.bloom.anno.PropertyTemplateProperty(name="Username", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Password", type=com.bloom.security.Password.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="ConnectionURL", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Tables", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="ExcludedTables", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="Query", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="FetchSize", type=Integer.class, required=false, defaultValue="100")}, inputType=WAEvent.class)
public class DatabaseReader_1_0
  extends SourceProcess
  implements SourceMetadataProvider
{
  private static Logger logger = Logger.getLogger(DatabaseReader_1_0.class);
  private boolean eofWait = true;
  private DatabaseReader dr = null;
  
  public void init(Map<String, Object> properties)
    throws Exception
  {
    super.init(properties);
    if (properties.containsKey("noeofwait")) {
      this.eofWait = false;
    }
    if ((properties.containsKey("Query")) && (properties.get("Query") != null) && (((String)properties.get("Query")).length() > 0)) {
      this.dr = new DatabaseReaderOld();
    } else {
      this.dr = new DatabaseReaderNew();
    }
    this.dr.initDR(properties);
  }
  
  public synchronized void init(Map<String, Object> properties, Map<String, Object> properties2, UUID sourceUUID, String distributionId)
    throws Exception
  {
    properties.put("sourceUUID", sourceUUID);
    if (properties.containsKey("noeofwait")) {
      this.eofWait = false;
    }
    if ((properties.containsKey("Query")) && (properties.get("Query") != null) && (((String)properties.get("Query")).length() > 0)) {
      this.dr = new DatabaseReaderOld();
    } else {
      this.dr = new DatabaseReaderNew();
    }
    this.dr.initDR(properties);
    super.init(properties, properties2, sourceUUID, distributionId);
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    if (logger.isTraceEnabled()) {
      logger.trace("DatabaseReader_1_0:receiveImpl called");
    }
    try
    {
      WAEvent evt = this.dr.nextEvent(this.sourceUUID);
      if (evt != null)
      {
        if (this.sourceUUID != null) {
          evt.typeUUID = ((UUID)this.typeUUIDForCDCTables.get(this.dr.getCurrentTableName()));
        }
        send(evt, 0);
      }
      else
      {
        try
        {
          Thread.sleep(100L);
        }
        catch (InterruptedException ie)
        {
          close();
          throw new ConnectionException("Stopping the Source thread has been interrupted.");
        }
      }
    }
    catch (NoSuchElementException ex)
    {
      try
      {
        if (this.eofWait)
        {
          Thread.sleep(100L);
        }
        else
        {
          WAEvent out = new WAEvent(0, this.sourceUUID);
          out.data = null;
          send(out, 0);
        }
      }
      catch (InterruptedException ie)
      {
        close();
        if (logger.isInfoEnabled()) {
          logger.info("DatabaseReader stopping gracefully as the thread has been interrupted");
        }
      }
    }
  }
  
  public void close()
    throws Exception
  {
    this.dr.close();
    super.close();
  }
  
  public static void main(String[] args)
    throws Exception
  {
    Map<String, Object> props = new HashMap();
    
    props.put("ConnectionURL", "jdbc:oracle:thin:@192.168.123.145:1521/XE");
    props.put("Username", "scott");
    props.put("Password", "tiger");
    
    props.put("FetchSize", Integer.valueOf(2000));
    props.put("Tables", " SCOTT.AUTHORIZATIONS_%");
    props.put("ExcludedTables", "AUTHORIZATIONSOLD");
    
    DatabaseReader_1_0 source = new DatabaseReader_1_0();
    try
    {
      source.init(props, props, new UUID(System.currentTimeMillis()), "qqqq");
      source.addEventSink(new AbstractEventSink()
      {
        public void receive(int channel, Event event)
          throws Exception
        {
          System.out.println(event.toString());
        }
      });
      for (int i = 0; i < 10; i++) {
        source.receive(0, null);
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  public String getMetadataKey()
  {
    return this.dr.getMetadataKey();
  }
  
  public Map<String, TypeDefOrName> getMetadata()
    throws Exception
  {
    return this.dr.getMetadata();
  }
}
