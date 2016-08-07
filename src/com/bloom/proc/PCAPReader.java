package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import com.bloom.proc.events.PCAPPacketEvent;
import com.bloom.recovery.SourcePosition;
import com.bloom.runtime.components.Flow;
import com.bloom.uuid.UUID;
import java.io.File;
import java.io.FileFilter;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.pcap4j.core.BpfProgram.BpfCompileMode;
import org.pcap4j.core.NotOpenException;
import org.pcap4j.core.PacketListener;
import org.pcap4j.core.PcapAddress;
import org.pcap4j.core.PcapHandle;
import org.pcap4j.core.PcapNativeException;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.core.PcapNetworkInterface.PromiscuousMode;
import org.pcap4j.core.Pcaps;
import org.pcap4j.packet.Packet;

@PropertyTemplate(name="PCAPReader", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="library", type=String.class, required=false, defaultValue="/usr/lib/libpcap.dylib"), @com.bloom.anno.PropertyTemplateProperty(name="interface", type=String.class, required=false, defaultValue="en0"), @com.bloom.anno.PropertyTemplateProperty(name="filter", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="timeout", type=Integer.class, required=false, defaultValue="10"), @com.bloom.anno.PropertyTemplateProperty(name="snaplen", type=Integer.class, required=false, defaultValue="65536"), @com.bloom.anno.PropertyTemplateProperty(name="live", type=Boolean.class, required=false, defaultValue="true"), @com.bloom.anno.PropertyTemplateProperty(name="directory", type=String.class, required=false, defaultValue="."), @com.bloom.anno.PropertyTemplateProperty(name="file", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="wildcard", type=Boolean.class, required=false, defaultValue="true")}, inputType=PCAPPacketEvent.class, requiresParser=false)
public class PCAPReader
  extends SourceProcess
{
  private static Logger logger = Logger.getLogger(PCAPReader.class);
  
  public void init(Map<String, Object> prop1, Map<String, Object> prop2, UUID uuid, String distributionID, SourcePosition startPosition, boolean sendPositions, Flow flow)
    throws Exception
  {
    String pcapLibName = "/usr/lib/libpcap.dylib";
    String propsPcapLibName = (String)prop1.get("library");
    if (propsPcapLibName != null) {
      pcapLibName = propsPcapLibName;
    }
    System.setProperty("org.pcap4j.core.pcapLibName", pcapLibName);
    
    Object liveVal = prop1.get("live");
    boolean live = (liveVal == null) || (liveVal.toString().equalsIgnoreCase("true"));
    if (live)
    {
      PcapNetworkInterface pni = null;
      List<PcapNetworkInterface> allDevs = null;
      try
      {
        allDevs = Pcaps.findAllDevs();
      }
      catch (PcapNativeException e)
      {
        throw new RuntimeException("Problem obtaining list of network devices: " + e.getMessage());
      }
      if ((allDevs == null) || (allDevs.size() == 0)) {
        throw new RuntimeException("No network device to capture from");
      }
      String ifName = (String)prop1.get("interface");
      for (PcapNetworkInterface apni : allDevs)
      {
        if (ifName.equals(apni.getName())) {
          pni = apni;
        } else {
          for (PcapAddress address : apni.getAddresses())
          {
            String addrString = address.getAddress().toString();
            if ((ifName.equalsIgnoreCase(addrString)) || (("/" + ifName).equalsIgnoreCase(addrString)))
            {
              pni = apni;
              break;
            }
          }
        }
        if (pni != null) {
          break;
        }
      }
      if (pni == null) {
        throw new RuntimeException("Could not find a network device matching: " + ifName);
      }
      Integer timeout = Integer.valueOf(Integer.parseInt(prop1.get("timeout").toString()));
      Integer snaplen = Integer.valueOf(Integer.parseInt(prop1.get("snaplen").toString()));
      this.pcapLoop = new PCAPLoop(pni, snaplen, timeout);
    }
    else
    {
      Object wildcardVal = prop1.get("wildcard");
      boolean wildcard = (wildcardVal == null) || (wildcardVal.toString().equalsIgnoreCase("true"));
      String directory = (String)prop1.get("directory");
      File directoryFile = new File(directory);
      if (!directoryFile.exists()) {
        throw new RuntimeException("Directory " + directory + " does not exist");
      }
      String file = (String)prop1.get("file");
      if (!wildcard)
      {
        File fileFile = new File(directoryFile.getAbsolutePath() + File.separatorChar + file);
        if (!fileFile.exists()) {
          throw new RuntimeException("Non wild-card file " + fileFile.getAbsolutePath() + " does not exist");
        }
      }
      this.pcapLoop = new PCAPLoop(directory, file, wildcard);
    }
    String filter = (String)prop1.get("filter");
    this.pcapLoop.setFilter(filter);
    
    this.pcapLoop.start();
  }
  
  PCAPLoop pcapLoop = null;
  
  class PCAPLoop
    extends Thread
  {
    PcapHandle handle = null;
    long lastFileTs = 0L;
    PcapNetworkInterface pni;
    Integer snaplen;
    Integer timeout;
    boolean wildcard;
    String file;
    String directory;
    File directoryFile;
    String filter;
    String filename = null;
    
    private PcapHandle getHandle()
    {
      return this.handle;
    }
    
    PacketListener listener = new PacketListener()
    {
      public void gotPacket(Packet packet)
      {
        long ts = PCAPReader.PCAPLoop.this.getHandle().getTimestampInts().longValue() * 1000L;
        int tsns = PCAPReader.PCAPLoop.this.getHandle().getTimestampMicros().intValue() * 1000;
        byte[] bytes = packet.getRawData();
        
        PCAPPacketEvent ppe = new PCAPPacketEvent(ts, tsns, bytes.length, packet);
        try
        {
          PCAPReader.this.send(ppe, 0);
        }
        catch (Exception e) {}
      }
    };
    
    public PCAPLoop(PcapNetworkInterface pni, Integer snaplen, Integer timeout)
    {
      this.pni = pni;
      this.snaplen = snaplen;
      this.timeout = timeout;
      setName("PCAP-Live-Monitor-Thread");
    }
    
    public PCAPLoop(String directory, String file, boolean wildcard)
    {
      this.directory = directory;
      this.directoryFile = new File(directory);
      this.file = file;
      this.wildcard = wildcard;
      setName("PCAP-SaveFile-Monitor-Thread");
    }
    
    public void setFilter(String filter)
    {
      this.filter = filter;
    }
    
    private void addFilterToHandle()
    {
      if ((this.filter != null) && (this.filter.length() != 0) && (!"null".equalsIgnoreCase(this.filter))) {
        try
        {
          this.handle.setFilter(this.filter, BpfProgram.BpfCompileMode.OPTIMIZE);
        }
        catch (PcapNativeException|NotOpenException e)
        {
          PCAPReader.logger.error("Could not apply pcap filter " + this.filter, e);
          this.running = false;
        }
      }
    }
    
    private void doLiveLoop()
    {
      if (this.handle == null)
      {
        try
        {
          this.handle = this.pni.openLive(this.snaplen == null ? 65536 : this.snaplen.intValue(), PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, this.timeout == null ? 10 : this.timeout.intValue());
        }
        catch (PcapNativeException e)
        {
          PCAPReader.logger.error("Could not open pcap handle", e);
          this.running = false;
        }
        addFilterToHandle();
      }
      try
      {
        this.handle.loop(100, this.listener);
      }
      catch (PcapNativeException|InterruptedException|NotOpenException e)
      {
        PCAPReader.logger.error("Problem processing live PCAP data", e);
      }
    }
    
    private void doFileLoop()
    {
      if (this.handle == null)
      {
        if (!this.wildcard)
        {
          if (this.filename == null)
          {
            this.filename = (this.directoryFile.getAbsolutePath() + File.separatorChar + this.file);
            try
            {
              this.handle = Pcaps.openOffline(this.filename);
            }
            catch (PcapNativeException e)
            {
              PCAPReader.logger.error("Could not open pcap handle for file " + this.filename, e);
              this.running = false;
              return;
            }
          }
          else
          {
            try
            {
              Thread.sleep(100L);
            }
            catch (InterruptedException e) {}
          }
        }
        else
        {
          File[] files = this.directoryFile.listFiles(new FileFilter()
          {
            public boolean accept(File afile)
            {
              return (afile.getName().startsWith(PCAPReader.PCAPLoop.this.file)) && (afile.lastModified() > PCAPReader.PCAPLoop.this.lastFileTs);
            }
          });
          if ((files != null) && (files.length > 0))
          {
            List<File> fileList = new ArrayList();
            for (File f : files) {
              fileList.add(f);
            }
            Collections.sort(fileList, new Comparator()
            {
              public int compare(File o1, File o2)
              {
                return Long.compare(o1.lastModified(), o2.lastModified());
              }
            });
            File aFile = (File)fileList.get(0);
            this.lastFileTs = aFile.lastModified();
            this.filename = aFile.getAbsolutePath();
            try
            {
              this.handle = Pcaps.openOffline(this.filename);
            }
            catch (PcapNativeException e)
            {
              PCAPReader.logger.error("Could not open pcap handle for file " + this.filename, e);
              this.running = false;
              return;
            }
          }
          else
          {
            try
            {
              Thread.sleep(100L);
              return;
            }
            catch (InterruptedException e) {}
          }
        }
        addFilterToHandle();
        try
        {
          this.handle.loop(0, this.listener);
        }
        catch (PcapNativeException|InterruptedException|NotOpenException e)
        {
          PCAPReader.logger.error("Problem reading from pcap file " + this.filename, e);
          this.running = false;
        }
        finally
        {
          this.handle.close();
          this.handle = null;
        }
      }
    }
    
    boolean running = true;
    
    public void run()
    {
      while (this.running) {
        if (this.pni != null) {
          doLiveLoop();
        } else {
          doFileLoop();
        }
      }
    }
    
    public void stopRunning()
    {
      this.running = false;
      try
      {
        if (this.handle != null)
        {
          this.handle.breakLoop();
          this.handle.close();
          this.handle = null;
        }
      }
      catch (NotOpenException e) {}
    }
  }
  
  public synchronized void receiveImpl(int channel, Event out)
    throws Exception
  {
    try
    {
      Thread.sleep(100L);
    }
    catch (Exception e) {}
  }
  
  public void close()
    throws Exception
  {
    if (this.pcapLoop != null) {
      this.pcapLoop.stopRunning();
    }
  }
}
