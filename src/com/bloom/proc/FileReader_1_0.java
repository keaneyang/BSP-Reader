package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import com.bloom.proc.events.WAEvent;
import com.bloom.runtime.BaseServer;
import com.bloom.source.lib.reader.Reader;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@PropertyTemplate(name="FileReader", type=AdapterType.source, properties={@com.bloom.anno.PropertyTemplateProperty(name="directory", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="wildcard", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="blocksize", type=Integer.class, required=false, defaultValue="64"), @com.bloom.anno.PropertyTemplateProperty(name="positionbyeof", type=Boolean.class, required=false, defaultValue="true"), @com.bloom.anno.PropertyTemplateProperty(name="skipbom", type=Boolean.class, required=false, defaultValue="true"), @com.bloom.anno.PropertyTemplateProperty(name="rolloverstyle", type=String.class, required=false, defaultValue="Default"), @com.bloom.anno.PropertyTemplateProperty(name="compressiontype", type=String.class, required=false, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="rolloverstyle", type=String.class, required=false, defaultValue="Default"), @com.bloom.anno.PropertyTemplateProperty(name="charset", type=String.class, required=false, defaultValue="UTF-8")}, inputType=WAEvent.class, requiresParser=true)
public class FileReader_1_0
  extends BaseReader
{
  public FileReader_1_0()
  {
    this.readerType = Reader.FILE_READER;
  }
  
  public static void main(String[] args)
  {
    Map<String, Object> mp = new HashMap();
    Map<String, Object> mp1 = new HashMap();
    
    mp.put("directory", "/Users/Vijay/Documents/Bloom/intellij/Product/Samples/AppData");
    mp.put("wildcard", "customerdetails.csv");
    mp.put("blockSize", Integer.valueOf(64));
    
    mp.put("positionByEOF", Boolean.valueOf(false));
    mp.put("charset", "UTF-8");
    
    mp1.put("handler", "DSVParser");
    mp1.put("header", Boolean.valueOf(false));
    mp1.put("rowdelimiter", "\n");
    mp1.put("columndelimiter", ",");
    
    AtomicInteger count = new AtomicInteger(0);
    final AtomicBoolean done = new AtomicBoolean(false);
    
    FileReader_1_0 file = new FileReader_1_0();
    long stime = System.currentTimeMillis();
    try
    {
      file.init(mp, mp1, null, BaseServer.getServerName(), null, false, null);
      file.addEventSink(new AbstractEventSink()
      {
        long lasttime = this.val$stime;
        int lastcount = 0;
        
        public void receive(int channel, Event event)
          throws Exception
        {
          WAEvent wa = (WAEvent)event;
          if (wa.data != null)
          {
            System.out.println(wa.data[0]);
            done.incrementAndGet();
            if ((done.get() != 0) && (done.get() % 100 == 0))
            {
              long etime = System.currentTimeMillis();
              int diff = (int)(etime - this.lasttime);
              int delta = done.get() - this.lastcount;
              int rate = delta * 1000 / diff;
              
              this.lasttime = etime;
              this.lastcount = done.get();
            }
          }
          else
          {
            this.val$done.set(true);
          }
        }
      });
      while (!done.get()) {
        file.receive(0, null);
      }
      return;
    }
    catch (Exception e) {}finally
    {
      try
      {
        file.close();
      }
      catch (Exception e)
      {
        e.printStackTrace();
      }
    }
  }
  
  public String getMetadataKey()
  {
    return null;
  }
}
