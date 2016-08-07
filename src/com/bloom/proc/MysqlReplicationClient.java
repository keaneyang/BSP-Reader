package com.bloom.proc;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.BinaryLogClient.LifecycleListener;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.FormatDescriptionEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.bloom.common.exc.ConnectionException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.MySQLSourcePosition;
import com.bloom.recovery.Position;
import com.bloom.runtime.CustomThreadFactory;
import com.bloom.runtime.meta.MetaInfo.Source;
import com.bloom.security.WASecurityManager;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.mysql.MysqlProperty;
import com.bloom.source.mysql.ParseMySQLEvent;
import com.bloom.uuid.UUID;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.log4j.Logger;

public class MysqlReplicationClient
  implements BinaryLogClient.EventListener, BinaryLogClient.LifecycleListener, Runnable
{
  private static final Logger logger = Logger.getLogger(MysqlReplicationClient.class);
  private MysqlProperty prop;
  private SourceProcess sourceProcess;
  private UUID sourceUUID;
  private String distributionId = null;
  private boolean sendPositions = false;
  private String binlogName = null;
  private String previousBinlogName = null;
  private Long binlogPosition = Long.valueOf(4L);
  private Long txnBeginPosition = Long.valueOf(0L);
  private HashMap<String, ArrayList<Integer>> tablenameHash = null;
  private HashMap<String, String> tablenameCaseMapping = null;
  private boolean sendBeforeImage;
  private boolean filterTransactionBoundaries;
  private String ipaddress;
  private int portno;
  private int server_id_bits;
  private MetaInfo.Source sourceMeta = null;
  private BinaryLogClient client;
  private ParseMySQLEvent parseEvent = new ParseMySQLEvent();
  private ExecutorService threadPool;
  
  public MysqlReplicationClient(MysqlProperty prop, String ipaddress, int portno, String userName, String password)
    throws Exception
  {
    this.prop = prop;
    this.binlogName = prop.getbinlogFileName();
    this.binlogPosition = prop.getbinLogPosition();
    this.previousBinlogName = prop.getPreviousBinlogFileName();
    this.sourceUUID = ((UUID)prop.getMap().get(Property.SOURCE_UUID));
    this.distributionId = ((String)prop.getMap().get("distributionId"));
    this.sendPositions = ((Boolean)prop.getMap().get("sendPosition")).booleanValue();
    this.sourceProcess = ((SourceProcess)prop.getMap().get(BaseReader.SOURCE_PROCESS));
    this.ipaddress = ipaddress;
    this.portno = portno;
    this.sourceMeta = ((MetaInfo.Source)MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.sourceUUID, WASecurityManager.TOKEN));
    try
    {
      if (prop.getSchema() != null) {
        this.client = new BinaryLogClient(ipaddress, portno, prop.getSchema(), userName, password);
      } else {
        this.client = new BinaryLogClient(ipaddress, portno, userName, password);
      }
      if (this.binlogName != null) {
        this.client.setBinlogFilename(this.binlogName);
      }
      if (this.binlogPosition.longValue() >= 4L) {
        this.client.setBinlogPosition(this.binlogPosition.longValue());
      }
      this.client.setServerId(12345321L);
      this.client.registerEventListener(this);
      this.client.registerLifecycleListener(this);
    }
    catch (Exception e)
    {
      throw new ConnectionException("Failed to connect to MySQL Server on " + ipaddress + ":" + portno + ". Please make sure it's running.", e);
    }
  }
  
  public void init()
  {
    this.parseEvent.setServer_ID_Bits(this.server_id_bits);
    this.parseEvent.setBinaryLogClient(this.client);
    ThreadFactory mySQLThreadFactory = new CustomThreadFactory("MySQLReplicationClientThread-%d");
    this.threadPool = Executors.newCachedThreadPool(mySQLThreadFactory);
    this.threadPool.submit(this);
  }
  
  public void setupTablenameHash(List<String> tables, HashMap<String, ArrayList<Integer>> tableKeyColumns)
  {
    this.tablenameHash = new HashMap();
    this.tablenameCaseMapping = new HashMap();
    for (int i = 0; i < tables.size(); i++)
    {
      this.tablenameHash.put(tables.get(i), tableKeyColumns.get(tables.get(i)));
      this.tablenameCaseMapping.put(((String)tables.get(i)).toLowerCase(), tables.get(i));
    }
  }
  
  public void setSendBeforeImage(boolean sendBeforeImage)
  {
    this.sendBeforeImage = sendBeforeImage;
  }
  
  public void setFilterTransactionBoundaries(boolean filterTransactionBoundaries)
  {
    this.filterTransactionBoundaries = filterTransactionBoundaries;
  }
  
  public void setServer_ID_Bits(int server_id_bits)
  {
    this.server_id_bits = server_id_bits;
  }
  
  public void onEvent(Event event)
  {
    EventType evtType = event.getHeader().getEventType();
    if (logger.isDebugEnabled())
    {
      logger.debug("\n" + evtType + "\n");
      logger.debug(event.toString());
    }
    try
    {
      String currentTablename;
      ArrayList<Integer> keyCols;
      int rowCount;
      int seqno;
      switch (event.getHeader().getEventType())
      {
      case TABLE_MAP: 
        TableMapEventData data = (TableMapEventData)event.getData();
        currentTablename = data.getDatabase() + "." + data.getTable();
        if (this.tablenameHash.get(this.tablenameCaseMapping.get(currentTablename.toLowerCase())) != null) {
          this.parseEvent.updateTableMap(event);
        }
        break;
      case FORMAT_DESCRIPTION: 
        FormatDescriptionEventData data = (FormatDescriptionEventData)event.getData();
        logger.info("Server version " + data.getServerVersion() + " binlogVersion " + data.getBinlogVersion());
        break;
      case ROTATE: 
        RotateEventData data = (RotateEventData)event.getData();
        String logName = data.getBinlogFilename().trim();
        if (!logName.equals(this.binlogName))
        {
          this.previousBinlogName = (this.binlogName != null ? new String(this.binlogName) : null);
          this.binlogName = logName;
          this.binlogPosition = Long.valueOf(data.getBinlogPosition());
        }
        break;
      case WRITE_ROWS: 
      case EXT_WRITE_ROWS: 
      case UPDATE_ROWS: 
      case EXT_UPDATE_ROWS: 
      case DELETE_ROWS: 
      case EXT_DELETE_ROWS: 
        currentTablename = this.parseEvent.getTablenameFromRowChangeEvent(event);
        if (currentTablename != null)
        {
          keyCols = (ArrayList)this.tablenameHash.get(this.tablenameCaseMapping.get(currentTablename.toLowerCase()));
          if (keyCols != null)
          {
            rowCount = this.parseEvent.getRowCount(event);
            seqno = 0;
          }
        }
        break;
      case QUERY: 
      case XID: 
      case STOP: 
        while (seqno < rowCount)
        {
          WAEvent wEvent = this.parseEvent.parseMySQLEvent(event, this.sourceUUID, this.sendBeforeImage, seqno, keyCols);
          Position outPosition = null;
          if (this.sendPositions)
          {
            EventHeaderV4 header = (EventHeaderV4)event.getHeader();
            outPosition = buildPosition(Long.valueOf(header.getPosition()), seqno);
          }
          wEvent.typeUUID = this.sourceProcess.getTypeUUIDForCDCTables(((String)this.tablenameCaseMapping.get(currentTablename.toLowerCase())).replace('.', '_'));
          
          wEvent.metadata.put("TableName", this.tablenameCaseMapping.get(currentTablename.toLowerCase()));
          
          this.sourceProcess.send(wEvent, 0, outPosition);
          seqno++;
          continue;
          
          ArrayList<Integer> keyCols = null;
          seqno = 0;
          WAEvent wEvent = this.parseEvent.parseMySQLEvent(event, this.sourceUUID, this.sendBeforeImage, seqno, keyCols);
          if (wEvent != null)
          {
            outPosition = null;
            if (this.sendPositions)
            {
              EventHeaderV4 header = (EventHeaderV4)event.getHeader();
              if (wEvent.metadata.get("OperationName").equals("BEGIN")) {
                this.txnBeginPosition = Long.valueOf(header.getPosition());
              }
              outPosition = buildPosition(Long.valueOf(header.getPosition()), seqno);
            }
            if ((!wEvent.metadata.get("OperationName").equals("BEGIN")) || 
              (!this.filterTransactionBoundaries))
            {
              this.sourceProcess.send(wEvent, 0, outPosition);
              break;
              
              ArrayList<Integer> keyCols = null;
              seqno = 0;
              WAEvent wEvent = this.parseEvent.parseMySQLEvent(event, this.sourceUUID, this.sendBeforeImage, seqno, keyCols);
              if (wEvent != null)
              {
                outPosition = null;
                if (this.sendPositions)
                {
                  EventHeaderV4 header = (EventHeaderV4)event.getHeader();
                  outPosition = buildPosition(Long.valueOf(header.getPosition()), seqno);
                }
                if (!this.filterTransactionBoundaries)
                {
                  this.sourceProcess.send(wEvent, 0, outPosition);
                  break;
                  
                  ArrayList<Integer> keyCols = null;
                  seqno = 0;
                  WAEvent wEvent = this.parseEvent.parseMySQLEvent(event, this.sourceUUID, this.sendBeforeImage, seqno, keyCols);
                  if (wEvent != null)
                  {
                    outPosition = null;
                    if (this.sendPositions)
                    {
                      EventHeaderV4 header = (EventHeaderV4)event.getHeader();
                      outPosition = buildPosition(Long.valueOf(header.getPosition()), seqno);
                    }
                    this.sourceProcess.send(wEvent, 0, outPosition);
                  }
                }
              }
            }
          }
        }
      }
    }
    catch (Exception e)
    {
      logger.error("Exception while processing Binlog " + evtType + " event " + e + "Cause: " + e.getCause(), e);
    }
  }
  
  private Position buildPosition(Long position, int count)
  {
    this.binlogPosition = position;
    MySQLSourcePosition sp = new MySQLSourcePosition(this.binlogName, this.binlogPosition.longValue(), count, this.previousBinlogName, this.txnBeginPosition);
    return sp != null ? Position.from(this.sourceUUID, this.distributionId, sp) : null;
  }
  
  public void close()
  {
    try
    {
      if ((this.client != null) && (this.client.isConnected())) {
        this.client.disconnect();
      }
      this.threadPool.shutdownNow();
    }
    catch (IOException e)
    {
      logger.error("IOException in Binlog close " + e, e);
    }
  }
  
  public void onCommunicationFailure(BinaryLogClient arg0, Exception e)
  {
    logger.error("Binlog communication failure " + e, e);
    close();
  }
  
  public void onConnect(BinaryLogClient arg0)
  {
    logger.info("Connected to " + this.client.getServerId() + " at (" + this.ipaddress + "," + this.portno + ") and will be reading from " + this.client.getBinlogPosition() + " in binary log " + this.client.getBinlogFilename());
  }
  
  public void onDisconnect(BinaryLogClient arg0)
  {
    logger.info("Disconnected from the Server at (" + this.ipaddress + "," + this.portno + ") successfully.");
  }
  
  public void onEventDeserializationFailure(BinaryLogClient client, Exception e)
  {
    logger.error("Failed to deserialize an Event " + e, e);
  }
  
  public void run()
  {
    try
    {
      this.client.connect();
    }
    catch (Exception e)
    {
      String errMsg = "Exception from Binlog Reader " + e;
      logger.error(errMsg, e);
    }
    finally
    {
      close();
    }
  }
}
