package com.bloom.source.snmp;

import com.sun.jmx.snmp.SnmpCounter;
import com.sun.jmx.snmp.SnmpGauge;
import com.sun.jmx.snmp.SnmpInt;
import com.sun.jmx.snmp.SnmpIpAddress;
import com.sun.jmx.snmp.SnmpMessage;
import com.sun.jmx.snmp.SnmpOid;
import com.sun.jmx.snmp.SnmpPduTrap;
import com.sun.jmx.snmp.SnmpStatusException;
import com.sun.jmx.snmp.SnmpString;
import com.sun.jmx.snmp.SnmpUnsignedInt;
import com.sun.jmx.snmp.SnmpValue;
import com.sun.jmx.snmp.SnmpVarBind;
import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.lib.rs.ResultSet;
import com.bloom.source.smlite.SMEvent;
import com.bloom.utility.SnmpPayload;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;

public class SNMPResultSet
  extends ResultSet
{
  private static Logger logger = Logger.getLogger(SNMPResultSet.class);
  protected Reader dataSource;
  private SnmpMessage decoder;
  public static String TYPE = "Type";
  public static String VERSION = "Version";
  public static String COMMUNITY = "Community";
  public static String TIME_STAMP = "TrapTime";
  public static String AGENT_IP = "AgentIp";
  public static String AGENT_PORT = "AgentPort";
  public static String ENTERPRISE = "Enterprise";
  Map<String, BaseType> objectMap;
  HashMap<String, Object> metaMap;
  private Map<String, Object> mappedValues;
  private Map<Object, String> OIDAliasLookUp;
  
  public SNMPResultSet(Reader reader, Property prop)
    throws IOException, InterruptedException
  {
    super(reader, prop);
    this.dataSource = reader;
  }
  
  public void init()
    throws IOException, InterruptedException, AdapterException
  {
    super.Init();
    this.decoder = new SnmpMessage();
    this.metaMap = new HashMap();
    this.mappedValues = new HashMap();
    this.OIDAliasLookUp = new HashMap();
    initializeObjectMap();
    LoadOIDAlias(this.prop.aliasConfigFile);
  }
  
  public boolean onEvent(SMEvent eventData)
  {
    return false;
  }
  
  private Object getValue(SnmpVarBind varBind)
  {
    return null;
  }
  
  public Map<String, Object> getResultAsMap()
  {
    return this.mappedValues;
  }
  
  public HashMap<String, Object> getMetaAsMap()
  {
    return this.metaMap;
  }
  
  public Constant.recordstatus next()
    throws IOException, InterruptedException
  {
    try
    {
      ByteBuffer packet = (ByteBuffer)this.dataSource.readBlock();
      if (packet != null)
      {
        this.decoder.decodeMessage(packet.array(), packet.array().length);
        this.mappedValues.clear();
        SnmpPduTrap pdu = (SnmpPduTrap)this.decoder.decodeSnmpPdu();
        
        this.metaMap.put(ENTERPRISE, pdu.enterprise.toString());
        this.metaMap.put(TYPE, SnmpPduTrap.pduTypeToString(pdu.type));
        this.metaMap.put(VERSION, Integer.valueOf(pdu.version));
        this.metaMap.put(COMMUNITY, new String(pdu.community, "UTF-8"));
        this.metaMap.put(AGENT_IP, pdu.agentAddr.stringValue());
        this.metaMap.put(AGENT_PORT, Integer.valueOf(pdu.port));
        this.metaMap.put(TIME_STAMP, Long.valueOf(pdu.timeStamp));
        
        SnmpVarBind[] var = pdu.varBindList;
        for (int itr = 0; itr < var.length; itr++)
        {
          String key = var[itr].getOid().toString();
          Object value = extractValue(var[itr]);
          this.mappedValues.put(key, value);
          
          String strippedKey = stripLastDecimal(key);
          this.mappedValues.put(strippedKey, value);
          String OIDAliasName = extractOIDAliasValue(strippedKey);
          if (OIDAliasName == null) {
            OIDAliasName = extractOIDAliasValue(key);
          }
          if (OIDAliasName != null) {
            this.mappedValues.put(OIDAliasName, value);
          }
        }
        return Constant.recordstatus.VALID_RECORD;
      }
    }
    catch (AdapterException|SnmpStatusException e)
    {
      e.printStackTrace();
      return Constant.recordstatus.ERROR_RECORD;
    }
    return Constant.recordstatus.NO_RECORD;
  }
  
  private String stripLastDecimal(String oidStr)
  {
    int idx = 0;
    if ((idx = oidStr.lastIndexOf('.')) != -1) {
      return oidStr.substring(0, idx);
    }
    return oidStr;
  }
  
  private Object extractValue(SnmpVarBind var)
  {
    BaseType type = (BaseType)this.objectMap.get(var.getSnmpValue().getTypeName());
    if (type == null)
    {
      logger.warn("Object type [" + var.getSnmpValue().getTypeName() + "] don't have appropriate handler");
      return null;
    }
    return type.getValue(var);
  }
  
  private String extractOIDAliasValue(Object key)
  {
    return (String)this.OIDAliasLookUp.get(key);
  }
  
  private void initializeObjectMap()
  {
    this.objectMap = new HashMap();
    
    this.objectMap.put("String", new SnmpString());
    this.objectMap.put("Integer", new Int());
    this.objectMap.put("TimeTicks", new TimeTick());
    this.objectMap.put("Object Identifier", new string());
    this.objectMap.put("Integer32", new Int());
    this.objectMap.put("Opaque", new Ignore());
    this.objectMap.put("Gauge32", new Gauge32());
    this.objectMap.put("Counter32", new Counter32());
  }
  
  abstract class BaseType
  {
    BaseType() {}
    
    public abstract Object getValue(SnmpVarBind paramSnmpVarBind);
  }
  
  class Int
    extends SNMPResultSet.BaseType
  {
    Int()
    {
      super();
    }
    
    public Object getValue(SnmpVarBind var)
    {
      return var.getSnmpIntValue().toInteger();
    }
  }
  
  class Counter32
    extends SNMPResultSet.BaseType
  {
    Counter32()
    {
      super();
    }
    
    public Object getValue(SnmpVarBind var)
    {
      return var.getSnmpCounterValue().toInteger();
    }
  }
  
  class Gauge32
    extends SNMPResultSet.BaseType
  {
    Gauge32()
    {
      super();
    }
    
    public Object getValue(SnmpVarBind var)
    {
      return var.getSnmpGaugeValue().toInteger();
    }
  }
  
  class string
    extends SNMPResultSet.BaseType
  {
    string()
    {
      super();
    }
    
    public Object getValue(SnmpVarBind var)
    {
      return var.getStringValue();
    }
  }
  
  class SnmpString
    extends SNMPResultSet.BaseType
  {
    SnmpString()
    {
      super();
    }
    
    public Object getValue(SnmpVarBind var)
    {
      SnmpPayload u = new SnmpPayload(var.getSnmpStringValue().byteValue(), var.getStringValue());
      return u;
    }
  }
  
  class TimeTick
    extends SNMPResultSet.BaseType
  {
    TimeTick()
    {
      super();
    }
    
    public Object getValue(SnmpVarBind var)
    {
      SnmpUnsignedInt tick = var.getSnmpTimeticksValue();
      return Integer.valueOf(tick.intValue());
    }
  }
  
  class Ignore
    extends SNMPResultSet.BaseType
  {
    Ignore()
    {
      super();
    }
    
    public Object getValue(SnmpVarBind var)
    {
      return new String("");
    }
  }
  
  private void LoadOIDAlias(String OIDAliasConfigFile)
    throws AdapterException
  {
    Properties prop = new Properties();
    InputStream input = null;
    if (OIDAliasConfigFile != null) {
      try
      {
        input = new FileInputStream(new File(OIDAliasConfigFile));
        prop.load(input);
        
        Enumeration<?> e = prop.propertyNames();
        while (e.hasMoreElements())
        {
          String key = (String)e.nextElement();
          this.OIDAliasLookUp.put(key.trim(), prop.getProperty(key).trim());
        }
        input.close();
      }
      catch (IOException e)
      {
        logger.error("Alias config file was not found at " + OIDAliasConfigFile);
        AdapterException se = new AdapterException(Error.FILE_NOT_FOUND, "Alias config file was not found at " + OIDAliasConfigFile);
        throw se;
      }
      catch (ClassCastException cc)
      {
        throw cc;
      }
    }
  }
}
