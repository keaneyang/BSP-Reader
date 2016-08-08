package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;
import com.bloom.common.exc.RecordException.Type;
import com.bloom.event.Event;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.CheckpointDetail;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.components.Flow;
import com.bloom.source.classloading.ParserLoader;
import com.bloom.source.lib.directory.FileBank;
import com.bloom.source.lib.directory.FileBank.Subject;
import com.bloom.source.lib.directory.MultiFileBank;
import com.bloom.source.lib.intf.CheckpointProvider;
import com.bloom.source.lib.intf.Parser;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.uuid.UUID;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.StandardWatchEventKinds;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Observable;
import java.util.Observer;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import org.apache.log4j.Logger;

@PropertyTemplate(name="MultiFileReader", type=AdapterType.source, properties={@com.bloom.anno.PropertyTemplateProperty(name="directory", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="wildcard", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="blocksize", type=Integer.class, required=false, defaultValue="64"), @com.bloom.anno.PropertyTemplateProperty(name="positionbyeof", type=Boolean.class, required=false, defaultValue="true"), @com.bloom.anno.PropertyTemplateProperty(name="skipbom", type=Boolean.class, required=false, defaultValue="true"), @com.bloom.anno.PropertyTemplateProperty(name="compressiontype", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="charset", type=String.class, required=false, defaultValue="UTF-8"), @com.bloom.anno.PropertyTemplateProperty(name="threadpoolsize", type=Integer.class, required=false, defaultValue="20"), @com.bloom.anno.PropertyTemplateProperty(name="yieldafter", type=Integer.class, required=false, defaultValue="20"), @com.bloom.anno.PropertyTemplateProperty(name="breakonnorecord", type=Boolean.class, required=false, defaultValue="false"), @com.bloom.anno.PropertyTemplateProperty(name="rolloverstyle", type=String.class, required=false, defaultValue="Default"), @com.bloom.anno.PropertyTemplateProperty(name="grouppattern", type=String.class, required=true, defaultValue="")}, inputType=WAEvent.class, requiresParser=true)
public class MultiFileReader_1_0
  extends BaseReader
  implements Observer
{
  UUID sourceRef;
  Map<String, Object> localCopyOfProperty;
  private ExecutorService executor;
  public boolean stopTheProcess;
  int parserCount;
  private Logger logger = Logger.getLogger(MultiFileReader_1_0.class);
  Map<String, Parser> processGroupMap;
  Map<String, Iterator<Event>> parserIteratorMap;
  Map<Integer, String> parserToGroupMap;
  MultiFileBank mfb;
  Property prop;
  LinkedBlockingQueue<Iterator<Event>> processQueue;
  Pattern groupPattern;
  boolean sendPositions;
  MFRPosition recoveryPosition;
  MFRPosition chkPntPosition;
  int yieldAfter;
  Map<String, Object> tmp;
  Map<String, String> recentlyAddedFile;
  int receiveImplCnt;
  long freeMem;
  long startTime;
  long endTime;
  long usedMeme;
  
  public MultiFileReader_1_0()
  {
    this.readerType = Reader.MULTI_FILE_READER;
    this.chkPntPosition = new MFRPosition();
  }
  
  public synchronized void close()
    throws Exception
  {
    synchronized (this.processGroupMap)
    {
      if (this.logger.isDebugEnabled()) {
        this.logger.debug("Got request to stop MultiFileReader");
      }
      if (!this.stopTheProcess)
      {
        this.stopTheProcess = true;
        if (this.executor != null) {
          this.executor.shutdown();
        }
        if (this.processGroupMap != null)
        {
          for (Map.Entry<String, Parser> entry : this.processGroupMap.entrySet())
          {
            if (this.logger.isDebugEnabled()) {
              this.logger.debug("Calling close for {" + (String)entry.getKey() + "}");
            }
            ((Parser)entry.getValue()).close();
          }
          if (this.logger.isDebugEnabled()) {
            this.logger.debug("Closed all process group");
          }
        }
        if (this.mfb != null)
        {
          this.mfb.stop(this.prop.wildcard);
          if (FileBank.instanceCnt.get() > 0) {
            this.logger.error("{" + FileBank.instanceCnt + "} number of Directory is active, unable to stop watcher thread");
          }
        }
      }
      else if (this.logger.isDebugEnabled())
      {
        this.logger.debug("Got request to stop on already stopped MultiFileReader");
      }
    }
  }
  
  public synchronized void init(Map<String, Object> prop1, Map<String, Object> prop2, UUID uuid, String distributionId, SourcePosition startPosition, boolean sendPositions, Flow flow)
    throws Exception
  {
    super.init(prop1, prop2, uuid, distributionId);
    
    this.localCopyOfProperty = new HashMap();
    
    this.sourceRef = uuid;
    this.sendPositions = sendPositions;
    this.recoveryPosition = ((MFRPosition)startPosition);
    if (this.recoveryPosition != null)
    {
      this.chkPntPosition.position.putAll(this.recoveryPosition.position);
      this.localCopyOfProperty.put("hasgotrecoveryposition", Boolean.valueOf(true));
    }
    for (Map.Entry<String, Object> entry : prop1.entrySet()) {
      this.localCopyOfProperty.put(((String)entry.getKey()).toLowerCase(), entry.getValue());
    }
    for (Map.Entry<String, Object> entry : prop2.entrySet()) {
      this.localCopyOfProperty.put(((String)entry.getKey()).toLowerCase(), entry.getValue());
    }
    this.localCopyOfProperty.put(BaseReader.SOURCE_PROCESS, this);
    this.localCopyOfProperty.put(Property.SOURCE_UUID, this.sourceRef);
    this.localCopyOfProperty.put(FileBank.OBSERVER, this);
    this.localCopyOfProperty.put(Property.READER_TYPE, Reader.FILE_READER);
    
    this.mfb = new MultiFileBank(new Property(this.localCopyOfProperty));
    this.mfb.addObserver(this);
    
    this.localCopyOfProperty.put(FileBank.INSTANCE, this.mfb);
    
    this.prop = new Property(this.localCopyOfProperty);
    this.yieldAfter = this.prop.getInt(Property.YIELD_AFTER, Property.DEFAULT_YIELD_AFTER);
    this.stopTheProcess = false;
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("MultiFileReader is initialized with following properties\nDirectory - [" + this.prop.directory + "]\n" + "Group Pattern - [" + this.prop.groupPattern + "]\n" + "BlockSize - [" + this.prop.blocksize + "]\n" + "Thread Pool Size - [" + this.prop.threadPoolSize + "]");
    }
    this.executor = Executors.newFixedThreadPool(this.prop.threadPoolSize, new MFRThreadFactory());
    this.processGroupMap = new TreeMap();
    this.parserIteratorMap = new TreeMap();
    this.parserToGroupMap = new TreeMap();
    this.processQueue = new LinkedBlockingQueue();
    this.recentlyAddedFile = new TreeMap();
    if (this.prop.groupPattern != null) {
      this.groupPattern = Pattern.compile(this.prop.groupPattern);
    }
    this.mfb.start();
  }
  
  public synchronized void receiveImpl(int channel, Event out)
    throws Exception
  {
    if (!this.stopTheProcess)
    {
      Iterator<Event> itr = (Iterator)this.processQueue.poll();
      if (itr != null)
      {
        Worker wrkr = new Worker(itr);
        this.executor.execute(wrkr);
        if (this.receiveImplCnt++ >= this.prop.threadPoolSize)
        {
          this.receiveImplCnt = 0;
          Thread.sleep(50L);
        }
      }
      else
      {
        Thread.sleep(50L);
      }
    }
  }
  
  public void update(Observable o, Object arg)
  {
    synchronized (this.processGroupMap)
    {
      if ((this.groupPattern != null) && (!this.stopTheProcess))
      {
        FileBank.Subject sub = (FileBank.Subject)arg;
        String fileName = sub.file.toString();
        String processGroup = FileBank.extractProcessGroup(this.prop.groupPattern, sub.file);
        Parser pParser = null;
        pParser = (Parser)this.processGroupMap.get(processGroup);
        if (sub.event == StandardWatchEventKinds.ENTRY_DELETE)
        {
          if (pParser != null) {
            try
            {
              if (this.mfb.isEmpty(processGroup)) {
                closeProcessGroup(pParser);
              }
            }
            catch (Exception e)
            {
              this.logger.warn("Got exception while closing process group", e);
            }
          }
        }
        else if (pParser == null)
        {
          if (this.logger.isDebugEnabled()) {
            this.logger.debug("Seen a new process group {" + processGroup + "}, going to create new parser for it.");
          }
          try
          {
            this.tmp = new TreeMap();
            this.tmp.putAll(this.localCopyOfProperty);
            if (this.logger.isDebugEnabled())
            {
              this.freeMem = Runtime.getRuntime().freeMemory();
              this.startTime = System.currentTimeMillis();
              this.logger.debug("Going to create parsre with { " + this.tmp.toString() + "}");
            }
            pParser = ParserLoader.createParser(this.tmp, this.sourceUUID);
            
            this.logger.debug("Going to add created parser into map");
            
            this.processGroupMap.put(processGroup, pParser);
            this.logger.debug("Adding parser for {" + processGroup + "}");
            
            String wildCard = processGroup;
            this.tmp.remove(Property.WILDCARD);
            this.tmp.put(Property.WILDCARD, wildCard);
            this.tmp.put("processgroup", processGroup);
            if (sub.event == StandardWatchEventKinds.ENTRY_CREATE) {
              this.tmp.put("positionbyeof", Boolean.valueOf(false));
            }
            if (this.logger.isDebugEnabled()) {
              this.logger.debug("Going to create reader for process group {" + processGroup + "} Property {" + this.tmp.toString() + "}");
            }
            Reader individualReader = Reader.createInstance(new Property(this.tmp));
            if (this.recoveryPosition != null)
            {
              CheckpointDetail recordCheckpoint = (CheckpointDetail)this.recoveryPosition.position.get(processGroup + "*");
              if (recordCheckpoint != null) {
                individualReader.position(recordCheckpoint, true);
              }
            }
            if (this.logger.isDebugEnabled()) {
              this.logger.debug("Calling parser.parse() for process group {" + processGroup + "}");
            }
            Iterator<Event> itr = pParser.parse(individualReader);
            this.parserIteratorMap.put(processGroup, itr);
            if (this.logger.isDebugEnabled()) {
              this.logger.debug("Updating Iterator map {" + processGroup + "}");
            }
            this.parserToGroupMap.put(Integer.valueOf(itr.hashCode()), processGroup);
            if (this.logger.isDebugEnabled()) {
              this.logger.debug("Updating Parser to Group map {" + processGroup + "}");
            }
            this.recentlyAddedFile.put(processGroup, fileName);
            
            this.processQueue.add(itr);
            if (this.logger.isDebugEnabled()) {
              this.logger.debug("Added Parser into scheduler queue {" + processGroup + "}");
            }
            if (this.logger.isDebugEnabled())
            {
              this.endTime = System.currentTimeMillis();
              this.usedMeme = Runtime.getRuntime().freeMemory();
              this.logger.debug("Time taken to create parser {" + processGroup + "} {" + this.parserCount++ + " } {" + (this.endTime - this.startTime) + "} memory used {" + (this.freeMem - this.usedMeme) + "}");
              this.logger.debug("Available memory : {" + Runtime.getRuntime().freeMemory() + "} Min memory : {" + Runtime.getRuntime().totalMemory() + "} Max memory {" + Runtime.getRuntime().maxMemory() + "}");
            }
            Thread.sleep(100L);
          }
          catch (Exception e)
          {
            e.printStackTrace();
            this.logger.error("Got exception while loading specified parser");
            if (this.logger.isDebugEnabled()) {
              this.logger.debug("Property map dump {" + this.localCopyOfProperty.toString() + "}");
            }
          }
        }
      }
    }
  }
  
  protected void closeProcessGroup(Parser parser)
    throws Exception
  {
    synchronized (this.processGroupMap)
    {
      String group = (String)this.parserToGroupMap.get(Integer.valueOf(parser.hashCode()));
      if (group != null)
      {
        if (this.logger.isDebugEnabled()) {
          this.logger.debug("Closing process group {" + group + "}");
        }
        Parser tmpParser = (Parser)this.parserIteratorMap.get(group);
        if (tmpParser != null)
        {
          if (tmpParser.hashCode() != parser.hashCode())
          {
            this.logger.warn("Got differnt parser reference, not going to close");
            return;
          }
          parser.close();
          this.processGroupMap.remove(group);
          this.parserToGroupMap.remove(Integer.valueOf(parser.hashCode()));
          this.parserIteratorMap.remove(group);
          this.recentlyAddedFile.remove(group);
        }
      }
      else
      {
        this.logger.warn("Could not get processGroup for {" + parser.toString() + "}");
      }
    }
  }
  
  public class Worker
    implements Runnable
  {
    Iterator<Event> itr;
    
    Worker()
    {
      this.itr = pItr;
    }
    
    public void run()
    {
      boolean doNotSchdeule = false;
      try
      {
        if (MultiFileReader_1_0.this.stopTheProcess != true)
        {
          try
          {
            int eventCnt = 0;
            while ((!MultiFileReader_1_0.this.stopTheProcess) && (eventCnt < MultiFileReader_1_0.this.yieldAfter) && 
              (this.itr.hasNext()))
            {
              Event event = (Event)this.itr.next();
              Position pos = null;
              if (MultiFileReader_1_0.this.sendPositions)
              {
                MultiFileReader_1_0.MFRPosition srcPosition = MultiFileReader_1_0.this.getPosition((CheckpointProvider)this.itr);
                pos = Position.from(MultiFileReader_1_0.this.sourceUUID, MultiFileReader_1_0.this.distributionID, srcPosition);
              }
              MultiFileReader_1_0.this.send(event, 0, pos);
              eventCnt++;
            }
            if ((eventCnt == MultiFileReader_1_0.this.yieldAfter) && (MultiFileReader_1_0.this.logger.isDebugEnabled()))
            {
              String pGroup = (String)MultiFileReader_1_0.this.parserToGroupMap.get(Integer.valueOf(this.itr.hashCode()));
              pGroup = pGroup == null ? "null" : pGroup;
              MultiFileReader_1_0.this.logger.debug("Process group {" + pGroup + "} is giving up CPU for others");
            }
          }
          catch (AdapterException|RecordException exp)
          {
            String errMsg = "Got exception while parsing data, exception message {" + exp.getMessage() + "}";
            MultiFileReader_1_0.this.logger.warn(errMsg);
          }
          catch (RuntimeException runtimeExp)
          {
            Throwable t = runtimeExp.getCause();
            if (((t instanceof RecordException)) || ((t instanceof AdapterException)))
            {
              if (((t instanceof RecordException)) && (((RecordException)t).type() == RecordException.Type.END_OF_DATASOURCE))
              {
                MultiFileReader_1_0.this.logger.warn("As the data source is closed, going to close the parser too");
                MultiFileReader_1_0.this.closeProcessGroup((Parser)this.itr);
                return;
              }
              MultiFileReader_1_0.this.logger.warn(t.getMessage());
            }
          }
          if (!MultiFileReader_1_0.this.stopTheProcess)
          {
            if (MultiFileReader_1_0.this.parserToGroupMap.get(Integer.valueOf(this.itr.hashCode())) != null) {
              MultiFileReader_1_0.this.processQueue.add(this.itr);
            } else if (MultiFileReader_1_0.this.logger.isDebugEnabled()) {
              MultiFileReader_1_0.this.logger.debug("Parser is closed not going to schedule.");
            }
            Thread.yield();
          }
          else if (MultiFileReader_1_0.this.logger.isDebugEnabled())
          {
            MultiFileReader_1_0.this.logger.debug("Stop is called, worker thread is exiting");
          }
        }
      }
      catch (Exception e)
      {
        MultiFileReader_1_0.this.logger.warn(e.getMessage());
      }
    }
  }
  
  public Position getCheckpoint()
  {
    if (this.sourceUUID == null)
    {
      if (this.logger.isDebugEnabled()) {
        this.logger.debug("Got null UUID");
      }
      return null;
    }
    Position result = null;
    synchronized (this.chkPntPosition)
    {
      result = Position.from(this.sourceUUID, this.distributionID, this.chkPntPosition);
    }
    return result;
  }
  
  private MFRPosition getPosition(CheckpointProvider provider)
  {
    CheckpointDetail chkPnt = provider.getCheckpointDetail();
    String group = (String)this.parserToGroupMap.get(Integer.valueOf(provider.hashCode()));
    group = group + '*';
    MFRPosition mfrPos = null;
    synchronized (this.chkPntPosition)
    {
      this.chkPntPosition.position.put(group, chkPnt);
      this.chkPntPosition.updateGroup = group;
      mfrPos = new MFRPosition(this.chkPntPosition);
    }
    return mfrPos;
  }
  
  public class MFRThreadFactory
    implements ThreadFactory
  {
    int threadCnt;
    
    public MFRThreadFactory()
    {
      this.threadCnt = 0;
    }
    
    public Thread newThread(Runnable r)
    {
      Thread thr = new Thread(r, "MFR_Executor_Pool_Thread_" + this.threadCnt++);
      thr.setPriority(1);
      return thr;
    }
  }
  
  public static class MFRPosition
    extends SourcePosition
  {
    private static final long serialVersionUID = 5501761434305045684L;
    public Map<String, CheckpointDetail> position;
    public String updateGroup;
    
    public MFRPosition()
    {
      this.position = new TreeMap();
    }
    
    public MFRPosition(CheckpointDetail checkpoint)
    {
      String fileName = checkpoint.getSourceName();
      this.position = new TreeMap();
      this.position.put(fileName, checkpoint);
      this.updateGroup = fileName;
    }
    
    public MFRPosition(MFRPosition mfr)
    {
      this.position = new TreeMap();
      this.position.putAll(mfr.position);
      this.updateGroup = mfr.updateGroup;
    }
    
    public int compareTo(SourcePosition arg0)
    {
      MFRPosition mfrPosition = (MFRPosition)arg0;
      int diff = 0;
      if ((this.updateGroup != null) && (mfrPosition.updateGroup == null)) {
        return 1;
      }
      CheckpointDetail thatChkPnt = (CheckpointDetail)mfrPosition.position.get(this.updateGroup);
      if (thatChkPnt == null) {
        return 1;
      }
      CheckpointDetail thisChkPnt = (CheckpointDetail)this.position.get(this.updateGroup);
      diff = thisChkPnt.compareTo(thatChkPnt);
      return diff;
    }
    
    public String toString()
    {
      return this.position.toString();
    }
  }
  
  public static void main(String[] args)
    throws IOException, InterruptedException
  {
    Logger logger = Logger.getLogger(Object.class);
    
    logger.debug("Starting standalone application");
    
    Map<String, Object> property = new TreeMap();
    property.put(Property.DIRECTORY, args[0]);
    int blockSize = Integer.parseInt(args[1]);
    String parserName = args[2];
    boolean positionByEof = args[3].toLowerCase().equals("true");
    int poolSize = Integer.parseInt(args[4]);
    
    logger.debug(" ==>" + args[0] + ", " + blockSize + ", " + parserName + ", " + positionByEof + "(" + args[3] + ")" + ", " + poolSize + " <===");
    
    property.put(Property.WILDCARD, "transaction*.log");
    
    property.put(Property.READER_TYPE, Reader.MULTI_FILE_READER);
    property.put(Property.NETWORK_FILE_SYSTEM, Boolean.valueOf(true));
    property.put(Property.GROUP_PATTERN, "\\*");
    property.put("blockSize", Integer.valueOf(blockSize));
    property.put("positionByEOF", Boolean.valueOf(positionByEof));
    property.put("charset", "UTF-8");
    property.put("threadpoolsize", Integer.valueOf(poolSize));
    
    property.put("handler", parserName);
    
    property.put("RecordBegin", ",closeAccount,");
    property.put("RecordEnd", "\n");
    
    property.put("separator", "~");
    
    logger.debug("Property : {" + property.toString() + "}");
    
    logger.debug("Going to start MFR with {" + property.toString() + "}");
    try
    {
      MultiFileReader_1_0 mfr = new MultiFileReader_1_0();
      mfr.init(property, property, null, null, null, false, null);
      for (;;)
      {
        mfr.receive(0, null);
        Thread.sleep(100L);
      }
    }
    catch (Exception e)
    {
      System.out.println("Got exception:");
      e.printStackTrace();
      
      logger.debug("Existing the application");
    }
  }
}

