package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;
import com.bloom.common.exc.RecordException.Type;
import com.bloom.event.Event;
import com.bloom.proc.events.WAEvent;
import com.bloom.recovery.Position;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.components.Flow;
import com.bloom.source.classloading.ParserLoader;
import com.bloom.source.lib.intf.CharParser;
import com.bloom.source.lib.intf.CheckpointProvider;
import com.bloom.source.lib.intf.Parser;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.uuid.UUID;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.Logger;

@PropertyTemplate(name="TCPReader", type=AdapterType.source, properties={@com.bloom.anno.PropertyTemplateProperty(name="IPAddress", type=String.class, required=true, defaultValue="localhost"), @com.bloom.anno.PropertyTemplateProperty(name="portno", type=Integer.class, required=true, defaultValue="10000"), @com.bloom.anno.PropertyTemplateProperty(name="blocksize", type=Integer.class, required=false, defaultValue="64"), @com.bloom.anno.PropertyTemplateProperty(name="maxconcurrentclients", type=Integer.class, required=false, defaultValue="5"), @com.bloom.anno.PropertyTemplateProperty(name="compressiontype", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="charset", type=String.class, required=false, defaultValue="UTF-8")}, inputType=WAEvent.class, requiresParser=true)
public class TCPReader_1_0
  extends BaseReader
{
  UUID sourceRef;
  Map<String, Object> localCopyOfProperty;
  private ExecutorService executor;
  public boolean stopTheProcess;
  Listener listener;
  private Logger logger = Logger.getLogger(TCPReader_1_0.class);
  int port;
  String ipAddress;
  ServerSocketChannel serverSocket;
  
  public TCPReader_1_0()
  {
    this.readerType = Reader.TCP_READER;
  }
  
  public void close()
    throws Exception
  {
    this.stopTheProcess = true;
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("TCPReader listening at IPAddress " + this.ipAddress + " port no " + this.port + " is closed");
    }
  }
  
  public void init(Map<String, Object> prop1, Map<String, Object> prop2, UUID uuid, String distributionId, SourcePosition startPosition, boolean sendPositions, Flow flow)
    throws Exception
  {
    super.init(prop1, prop2, uuid, distributionId);
    this.sourceRef = uuid;
    this.localCopyOfProperty = new HashMap();
    this.localCopyOfProperty.putAll(prop1);
    this.localCopyOfProperty.putAll(prop2);
    this.localCopyOfProperty.put(BaseReader.SOURCE_PROCESS, this);
    this.localCopyOfProperty.put(Property.SOURCE_UUID, this.sourceRef);
    
    Property prop = new Property(this.localCopyOfProperty);
    this.port = prop.portno;
    this.ipAddress = prop.ipaddress;
    
    this.serverSocket = ServerSocketChannel.open();
    InetSocketAddress addr = new InetSocketAddress(this.ipAddress, this.port);
    this.serverSocket.bind(addr);
    this.serverSocket.configureBlocking(false);
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("TCPReader is using IPAddress " + this.ipAddress + " listening at " + this.port);
    }
    this.listener = new Listener(prop);
    
    this.stopTheProcess = false;
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("TCPReader is initialized with following properties\nIPAddress - [" + prop.ipaddress + "]\n" + "PortNo - [" + prop.portno + "]\n" + "BlockSize - [" + prop.blocksize + "]\n" + "Maximum Concurent client - [" + prop.maxConcurrentClients + "]");
    }
  }
  
  public synchronized void receiveImpl(int channel, Event out)
    throws Exception
  {
    try
    {
      Thread.sleep(100L);
    }
    catch (InterruptedException exp) {}
  }
  
  class Listener
    implements Runnable
  {
    Property prop;
    Iterator<SelectionKey> itr;
    SelectionKey event;
    Thread listenerThread;
    
    public Listener(Property prop)
    {
      this.prop = prop;
      this.listenerThread = new Thread(this);
      this.listenerThread.start();
    }
    
    public void run()
    {
      TCPReader_1_0.this.executor = Executors.newFixedThreadPool(this.prop.maxConcurrentClients);
      try
      {
        Selector selector = Selector.open();
        TCPReader_1_0.this.serverSocket.register(selector, 16);
        while (!TCPReader_1_0.this.stopTheProcess)
        {
          selector.select(100L);
          this.itr = selector.selectedKeys().iterator();
          while (this.itr.hasNext())
          {
            this.event = ((SelectionKey)this.itr.next());
            this.itr.remove();
            if (this.event.isAcceptable())
            {
              ServerSocketChannel serverChannel = (ServerSocketChannel)this.event.channel();
              SocketChannel clientSocket = serverChannel.accept();
              Map<String, Object> propCopy = new HashMap();
              propCopy.putAll(TCPReader_1_0.this.localCopyOfProperty);
              propCopy.put(Reader.READER_TYPE, Reader.STREAM_READER);
              propCopy.put(Reader.STREAM, clientSocket.socket().getInputStream());
              
              TCPReader_1_0.ClientHandler clientHandler = new TCPReader_1_0.ClientHandler(TCPReader_1_0.this, new Property(propCopy));
              TCPReader_1_0.this.executor.execute(clientHandler);
            }
          }
        }
        selector.close();
        if (TCPReader_1_0.this.serverSocket.isOpen()) {
          TCPReader_1_0.this.serverSocket.close();
        }
      }
      catch (IOException e)
      {
        TCPReader_1_0.this.logger.warn("TCPListener got I/O exception", e);
      }
    }
  }
  
  class ClientHandler
    implements Runnable
  {
    Property tmpProp;
    
    public ClientHandler(Property prop)
    {
      this.tmpProp = prop;
      if (TCPReader_1_0.this.logger.isTraceEnabled()) {
        TCPReader_1_0.this.logger.trace("A new connection has been established to Server at IPAddress " + prop.ipaddress + " at port no " + prop.portno + ". Thread id is " + Thread.currentThread().getId());
      }
    }
    
    public void run()
    {
      int sleepTime = 0;
      Position outPos = null;
      boolean supportCheckPointing = false;
      try
      {
        Parser parser = ParserLoader.loadParser(this.tmpProp.propMap, TCPReader_1_0.this.sourceRef);
        if (!(parser instanceof CharParser))
        {
          Map<String, Object> localMap = this.tmpProp.getMap();
          localMap.remove(Property.CHARSET);
          this.tmpProp = new Property(localMap);
        }
        Reader reader = Reader.createInstance(this.tmpProp);
        int maxSleepTime = reader.eofdelay();
        UUID sourceRef = (UUID)this.tmpProp.propMap.get(Property.SOURCE_UUID);
        SourceProcess sourceProcess = (SourceProcess)this.tmpProp.propMap.get(BaseReader.SOURCE_PROCESS);
        if ((sourceRef != null) && (sourceProcess != null))
        {
          if ((parser instanceof CheckpointProvider)) {
            supportCheckPointing = true;
          }
          Iterator<Event> itr = parser.parse(reader);
          for (;;)
          {
            if (!((TCPReader_1_0)sourceProcess).stopTheProcess) {
              try
              {
                if (itr.hasNext())
                {
                  Event waEvent = (Event)itr.next();
                  if (supportCheckPointing) {
                    outPos = ((CheckpointProvider)parser).getPositionDetail();
                  }
                  if (sourceProcess != null)
                  {
                    sourceProcess.send(waEvent, 0, outPos);
                    sleepTime = 0;
                    continue;
                  }
                }
                sleepTime += sleepTime + 1;
                if (sleepTime > maxSleepTime) {
                  sleepTime = maxSleepTime;
                }
                Thread.sleep(sleepTime);
              }
              catch (RuntimeException exp)
              {
                Throwable t = exp.getCause();
                if (((t instanceof RecordException)) && 
                  (((RecordException)t).type() == RecordException.Type.END_OF_DATASOURCE)) {
                  break label280;
                }
              }
            }
          }
          label280:
          parser.close();
        }
      }
      catch (AdapterException e)
      {
        e.printStackTrace();
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }
  }
}
