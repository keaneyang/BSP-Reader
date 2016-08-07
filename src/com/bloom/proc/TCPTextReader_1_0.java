package com.bloom.proc;

import com.bloom.anno.AdapterType;
import com.bloom.anno.PropertyTemplate;
import com.bloom.event.Event;
import com.bloom.proc.events.StringArrayEvent;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Map;

@PropertyTemplate(name="TCPTextReader", type=AdapterType.internal, properties={@com.bloom.anno.PropertyTemplateProperty(name="port", type=String.class, required=true, defaultValue=""), @com.bloom.anno.PropertyTemplateProperty(name="nosplit", type=String.class, required=false, defaultValue="false")}, inputType=StringArrayEvent.class)
public class TCPTextReader_1_0
  extends SourceProcess
{
  private AsynchronousServerSocketChannel listener;
  private int port;
  private boolean nosplit = false;
  
  public void init(Map<String, Object> properties)
    throws Exception
  {
    super.init(properties);
    Object portObj = properties.get("port");
    if ((portObj instanceof String))
    {
      String portStr = (String)portObj;
      this.port = Integer.valueOf(portStr).intValue();
    }
    else if ((portObj instanceof Number))
    {
      Number num = (Number)portObj;
      this.port = num.intValue();
    }
    else
    {
      throw new RuntimeException("invalid type of <port> property");
    }
    Object noSplit = properties.get("nosplit");
    if (noSplit != null) {
      this.nosplit = Boolean.valueOf(noSplit.toString()).booleanValue();
    }
    this.listener = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(this.port));
    
    this.listener.accept(null, new CompletionHandler()
    {
      public void completed(AsynchronousSocketChannel ch, TCPTextReader_1_0 att)
      {
        TCPTextReader_1_0.this.listener.accept(null, this);
        
        TCPTextReader_1_0.this.handle(ch);
      }
      
      public void failed(Throwable exc, TCPTextReader_1_0 att)
      {
        if (!(exc instanceof AsynchronousCloseException)) {
          System.err.println("reader failed to accept connection:" + exc);
        }
      }
    });
  }
  
  private void handle(final AsynchronousSocketChannel ch)
  {
    ByteBuffer b = ByteBuffer.allocate(4096);
    ch.read(b, b, new CompletionHandler()
    {
      private final StringBuilder buffer = new StringBuilder();
      
      public void completed(Integer result, ByteBuffer buf)
      {
        if (result.intValue() == -1)
        {
          try
          {
            ch.close();
          }
          catch (IOException e)
          {
            throw new RuntimeException(e);
          }
          TCPTextReader_1_0.this.processLine(this.buffer);
          return;
        }
        if (result.intValue() != buf.position()) {
          throw new RuntimeException("number of bytes != buffer position");
        }
        char[] chars = new String(buf.array(), 0, buf.position()).toCharArray();
        buf.clear();
        for (char c : chars) {
          if (c == '\n') {
            TCPTextReader_1_0.this.processLine(this.buffer);
          } else {
            this.buffer.append(c);
          }
        }
        ch.read(buf, buf, this);
      }
      
      public void failed(Throwable exc, ByteBuffer buf)
      {
        System.err.println("reader failed to read connection:" + exc);
        try
        {
          ch.close();
        }
        catch (IOException e)
        {
          throw new RuntimeException(e);
        }
      }
    });
  }
  
  private void processLine(StringBuilder b)
  {
    if (b.length() == 0) {
      return;
    }
    String row = b.toString();
    b.setLength(0);
    String[] data = this.nosplit ? new String[] { row } : row.split("\\s+");
    StringArrayEvent event = new StringArrayEvent(System.currentTimeMillis());
    for (int i = 0; i < data.length; i++) {
      if (data[i].equals("null")) {
        data[i] = null;
      }
    }
    event.setData(data);
    try
    {
      send(event, 0);
    }
    catch (Exception e)
    {
      throw new RuntimeException(e);
    }
  }
  
  public void receiveImpl(int channel, Event event)
    throws Exception
  {
    synchronized (this)
    {
      wait();
    }
  }
  
  public void close()
    throws Exception
  {
    if (this.listener != null) {
      this.listener.close();
    }
    super.close();
  }
}

