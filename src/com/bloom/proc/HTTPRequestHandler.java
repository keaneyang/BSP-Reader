package com.bloom.proc;

import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import com.bloom.event.Event;
import com.bloom.exception.SecurityException;
import com.bloom.metaRepository.MetaDataRepositoryException;
import com.bloom.metaRepository.MetadataRepository;
import com.bloom.runtime.compiler.CompilerUtils;
import com.bloom.runtime.components.EntityType;
import com.bloom.runtime.meta.MetaInfo;
import com.bloom.runtime.meta.MetaInfo.Namespace;
import com.bloom.runtime.meta.MetaInfo.PropertyDef;
import com.bloom.runtime.meta.MetaInfo.PropertyTemplateInfo;
import com.bloom.security.Password;
import com.bloom.security.WASecurityManager;
import com.bloom.source.lib.intf.Parser;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.nvp.NVPProperty;
import com.bloom.source.nvp.NameValueParser;
import com.bloom.uuid.UUID;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Enumeration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

class HTTPRequestHandler
  extends AbstractHandler
{
  private Logger logger = Logger.getLogger(HTTPRequestHandler.class);
  private String contentType;
  private String encoding;
  private String charset;
  private String uriParserName;
  private final String COMPRESSION_TYPE = Property.COMPRESSION_TYPE;
  private final String CHARSET_PROPERTY = "charset";
  private final String ORIGINAL_PARSER_NAME = "parserName";
  private final String PARSER_NAME = "handler";
  private final String ENCODING = "Accept-Encoding";
  private final String CHARSET = "Accept-Charset";
  private final String CONTENT_TYPE = "Content-Type";
  private static final String SOURCE_UUID = "sourceuuid";
  boolean extractHeader = false;
  private Parser parser;
  Map<String, Object> propertyMap;
  private boolean extractURI = false;
  private UUID sourceUUID;
  BlockingQueue<Event> sendQueue;
  
  HTTPRequestHandler(Map<String, Object> propMap, BlockingQueue<Event> sendQueue)
  {
    this.propertyMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    this.propertyMap.putAll(propMap);
    String parserName = (String)this.propertyMap.get("parserName");
    if (parserName != null) {
      this.propertyMap.put("handler", parserName);
    } else {
      this.extractURI = true;
    }
    this.sourceUUID = ((UUID)this.propertyMap.get("sourceuuid"));
    this.sendQueue = sendQueue;
  }
  
  private static AtomicInteger requestCount = new AtomicInteger(0);
  private static AtomicInteger lastRequestCount = new AtomicInteger(0);
  private static AtomicLong requestTime = new AtomicLong(0L);
  private static AtomicLong lastOutTime = new AtomicLong(0L);
  
  private void dump(Request header)
  {
    Enumeration<?> headerNames = header.getHeaderNames();
    while (headerNames.hasMoreElements())
    {
      String headerName = (String)headerNames.nextElement();
      this.logger.debug("Header Name : [" + headerName + "] Header Value :[" + header.getHeader(headerName) + "]");
    }
  }
  
  /* Error */
  public void handle(String target, Request baseRequest, javax.servlet.http.HttpServletRequest request, javax.servlet.http.HttpServletResponse response)
    throws java.io.IOException, javax.servlet.ServletException
  {
    // Byte code:
    //   0: iconst_0
    //   1: istore 5
    //   3: invokestatic 45	java/lang/System:nanoTime	()J
    //   6: lstore 7
    //   8: aload_0
    //   9: getfield 4	com/bloom/proc/HTTPRequestHandler:logger	Lorg/apache/log4j/Logger;
    //   12: invokevirtual 46	org/apache/log4j/Logger:isDebugEnabled	()Z
    //   15: ifeq +51 -> 66
    //   18: aload_0
    //   19: getfield 4	com/bloom/proc/HTTPRequestHandler:logger	Lorg/apache/log4j/Logger;
    //   22: new 36	java/lang/StringBuilder
    //   25: dup
    //   26: invokespecial 37	java/lang/StringBuilder:<init>	()V
    //   29: ldc 47
    //   31: invokevirtual 39	java/lang/StringBuilder:append	(Ljava/lang/String;)Ljava/lang/StringBuilder;
    //   34: aload_2
    //   35: invokevirtual 48	org/eclipse/jetty/server/Request:getRemoteAddr	()Ljava/lang/String;
    //   38: invokevirtual 39	java/lang/StringBuilder:append	(Ljava/lang/String;)Ljava/lang/StringBuilder;
    //   41: ldc 42
    //   43: invokevirtual 39	java/lang/StringBuilder:append	(Ljava/lang/String;)Ljava/lang/StringBuilder;
    //   46: invokevirtual 43	java/lang/StringBuilder:toString	()Ljava/lang/String;
    //   49: invokevirtual 44	org/apache/log4j/Logger:debug	(Ljava/lang/Object;)V
    //   52: aload_0
    //   53: getfield 4	com/bloom/proc/HTTPRequestHandler:logger	Lorg/apache/log4j/Logger;
    //   56: ldc 49
    //   58: invokevirtual 44	org/apache/log4j/Logger:debug	(Ljava/lang/Object;)V
    //   61: aload_0
    //   62: aload_2
    //   63: invokespecial 50	com/bloom/proc/HTTPRequestHandler:dump	(Lorg/eclipse/jetty/server/Request;)V
    //   66: aload_0
    //   67: getfield 20	com/bloom/proc/HTTPRequestHandler:extractURI	Z
    //   70: ifeq +11 -> 81
    //   73: aload_0
    //   74: aload_2
    //   75: invokevirtual 51	org/eclipse/jetty/server/Request:getQueryString	()Ljava/lang/String;
    //   78: invokespecial 52	com/bloom/proc/HTTPRequestHandler:extractURI	(Ljava/lang/String;)V
    //   81: aload_0
    //   82: getfield 19	com/bloom/proc/HTTPRequestHandler:extractHeader	Z
    //   85: ifeq +8 -> 93
    //   88: aload_0
    //   89: aload_2
    //   90: invokespecial 53	com/bloom/proc/HTTPRequestHandler:extractHeaders	(Lorg/eclipse/jetty/server/Request;)V
    //   93: goto +15 -> 108
    //   96: astore 9
    //   98: new 55	java/io/IOException
    //   101: dup
    //   102: aload 9
    //   104: invokespecial 56	java/io/IOException:<init>	(Ljava/lang/Throwable;)V
    //   107: athrow
    //   108: aload_2
    //   109: invokevirtual 57	org/eclipse/jetty/server/Request:getInputStream	()Ljavax/servlet/ServletInputStream;
    //   112: astore 9
    //   114: new 58	java/util/HashMap
    //   117: dup
    //   118: invokespecial 59	java/util/HashMap:<init>	()V
    //   121: astore 11
    //   123: aload 11
    //   125: ldc 60
    //   127: aload_2
    //   128: invokevirtual 48	org/eclipse/jetty/server/Request:getRemoteAddr	()Ljava/lang/String;
    //   131: invokeinterface 28 3 0
    //   136: pop
    //   137: aload 11
    //   139: ldc 61
    //   141: aload_2
    //   142: invokevirtual 62	org/eclipse/jetty/server/Request:getProtocol	()Ljava/lang/String;
    //   145: invokeinterface 28 3 0
    //   150: pop
    //   151: aload 11
    //   153: ldc 63
    //   155: aload_2
    //   156: invokevirtual 64	org/eclipse/jetty/server/Request:getContentLength	()I
    //   159: invokestatic 65	java/lang/Integer:valueOf	(I)Ljava/lang/Integer;
    //   162: invokeinterface 28 3 0
    //   167: pop
    //   168: aload 11
    //   170: ldc 66
    //   172: new 36	java/lang/StringBuilder
    //   175: dup
    //   176: invokespecial 37	java/lang/StringBuilder:<init>	()V
    //   179: aload_2
    //   180: invokevirtual 67	org/eclipse/jetty/server/Request:getRequestURL	()Ljava/lang/StringBuffer;
    //   183: invokevirtual 68	java/lang/StringBuilder:append	(Ljava/lang/Object;)Ljava/lang/StringBuilder;
    //   186: aload_2
    //   187: invokevirtual 51	org/eclipse/jetty/server/Request:getQueryString	()Ljava/lang/String;
    //   190: ifnull +10 -> 200
    //   193: aload_2
    //   194: invokevirtual 51	org/eclipse/jetty/server/Request:getQueryString	()Ljava/lang/String;
    //   197: goto +5 -> 202
    //   200: ldc 69
    //   202: invokevirtual 39	java/lang/StringBuilder:append	(Ljava/lang/String;)Ljava/lang/StringBuilder;
    //   205: invokevirtual 43	java/lang/StringBuilder:toString	()Ljava/lang/String;
    //   208: invokeinterface 28 3 0
    //   213: pop
    //   214: aload 11
    //   216: ldc 70
    //   218: aload_2
    //   219: ldc 71
    //   221: invokevirtual 41	org/eclipse/jetty/server/Request:getHeader	(Ljava/lang/String;)Ljava/lang/String;
    //   224: ifnull +12 -> 236
    //   227: aload_2
    //   228: ldc 71
    //   230: invokevirtual 41	org/eclipse/jetty/server/Request:getHeader	(Ljava/lang/String;)Ljava/lang/String;
    //   233: goto +5 -> 238
    //   236: ldc 69
    //   238: invokeinterface 28 3 0
    //   243: pop
    //   244: aload_0
    //   245: getfield 24	com/bloom/proc/HTTPRequestHandler:propertyMap	Ljava/util/Map;
    //   248: getstatic 72	com/bloom/source/lib/reader/Reader:READER_TYPE	Ljava/lang/String;
    //   251: getstatic 73	com/bloom/source/lib/reader/Reader:STREAM_READER	Ljava/lang/String;
    //   254: invokeinterface 28 3 0
    //   259: pop
    //   260: aload_0
    //   261: getfield 24	com/bloom/proc/HTTPRequestHandler:propertyMap	Ljava/util/Map;
    //   264: getstatic 74	com/bloom/source/lib/reader/Reader:STREAM	Ljava/lang/String;
    //   267: aload 9
    //   269: invokeinterface 28 3 0
    //   274: pop
    //   275: aload_0
    //   276: getfield 24	com/bloom/proc/HTTPRequestHandler:propertyMap	Ljava/util/Map;
    //   279: ldc 75
    //   281: aload 11
    //   283: invokeinterface 28 3 0
    //   288: pop
    //   289: new 76	com/bloom/source/lib/prop/Property
    //   292: dup
    //   293: aload_0
    //   294: getfield 24	com/bloom/proc/HTTPRequestHandler:propertyMap	Ljava/util/Map;
    //   297: invokespecial 77	com/bloom/source/lib/prop/Property:<init>	(Ljava/util/Map;)V
    //   300: invokestatic 78	com/bloom/source/lib/reader/Reader:createInstance	(Lcom/bloom/source/lib/prop/Property;)Lcom/bloom/source/lib/reader/Reader;
    //   303: astore 10
    //   305: aload_0
    //   306: aload_0
    //   307: getfield 24	com/bloom/proc/HTTPRequestHandler:propertyMap	Ljava/util/Map;
    //   310: aload_0
    //   311: getfield 31	com/bloom/proc/HTTPRequestHandler:sourceUUID	Lcom/bloom/uuid/UUID;
    //   314: invokestatic 79	com/bloom/source/classloading/ParserLoader:loadParser	(Ljava/util/Map;Lcom/bloom/uuid/UUID;)Lcom/bloom/source/lib/intf/Parser;
    //   317: putfield 80	com/bloom/proc/HTTPRequestHandler:parser	Lcom/bloom/source/lib/intf/Parser;
    //   320: aload_0
    //   321: getfield 80	com/bloom/proc/HTTPRequestHandler:parser	Lcom/bloom/source/lib/intf/Parser;
    //   324: aload 10
    //   326: invokeinterface 81 2 0
    //   331: astore 12
    //   333: iconst_1
    //   334: istore 13
    //   336: aload 12
    //   338: invokeinterface 82 1 0
    //   343: ifeq +29 -> 372
    //   346: aload 12
    //   348: invokeinterface 83 1 0
    //   353: checkcast 84	com/bloom/event/Event
    //   356: astore 6
    //   358: aload_0
    //   359: getfield 32	com/bloom/proc/HTTPRequestHandler:sendQueue	Ljava/util/concurrent/BlockingQueue;
    //   362: aload 6
    //   364: invokeinterface 85 2 0
    //   369: goto +36 -> 405
    //   372: iload 13
    //   374: ifeq +9 -> 383
    //   377: iconst_0
    //   378: istore 13
    //   380: goto +25 -> 405
    //   383: aload_0
    //   384: getfield 4	com/bloom/proc/HTTPRequestHandler:logger	Lorg/apache/log4j/Logger;
    //   387: invokevirtual 86	org/apache/log4j/Logger:isTraceEnabled	()Z
    //   390: ifeq +12 -> 402
    //   393: aload_0
    //   394: getfield 4	com/bloom/proc/HTTPRequestHandler:logger	Lorg/apache/log4j/Logger;
    //   397: ldc 87
    //   399: invokevirtual 88	org/apache/log4j/Logger:trace	(Ljava/lang/Object;)V
    //   402: goto +77 -> 479
    //   405: ldc2_w 89
    //   408: invokestatic 91	java/lang/Thread:sleep	(J)V
    //   411: goto -75 -> 336
    //   414: astore 14
    //   416: aload 14
    //   418: invokevirtual 93	java/lang/RuntimeException:getCause	()Ljava/lang/Throwable;
    //   421: astore 15
    //   423: aload 15
    //   425: instanceof 94
    //   428: ifeq +24 -> 452
    //   431: aload 15
    //   433: checkcast 94	com/bloom/common/exc/RecordException
    //   436: astore 16
    //   438: aload 16
    //   440: invokevirtual 95	com/bloom/common/exc/RecordException:type	()Lcom/bloom/common/exc/RecordException$Type;
    //   443: getstatic 96	com/bloom/common/exc/RecordException$Type:END_OF_DATASOURCE	Lcom/bloom/common/exc/RecordException$Type;
    //   446: if_acmpne +6 -> 452
    //   449: goto +30 -> 479
    //   452: aload_0
    //   453: getfield 4	com/bloom/proc/HTTPRequestHandler:logger	Lorg/apache/log4j/Logger;
    //   456: invokevirtual 86	org/apache/log4j/Logger:isTraceEnabled	()Z
    //   459: ifeq +14 -> 473
    //   462: aload_0
    //   463: getfield 4	com/bloom/proc/HTTPRequestHandler:logger	Lorg/apache/log4j/Logger;
    //   466: ldc 97
    //   468: aload 14
    //   470: invokevirtual 98	org/apache/log4j/Logger:trace	(Ljava/lang/Object;Ljava/lang/Throwable;)V
    //   473: iconst_1
    //   474: istore 5
    //   476: goto +3 -> 479
    //   479: aload_0
    //   480: getfield 80	com/bloom/proc/HTTPRequestHandler:parser	Lcom/bloom/source/lib/intf/Parser;
    //   483: invokeinterface 99 1 0
    //   488: goto +15 -> 503
    //   491: astore 11
    //   493: new 55	java/io/IOException
    //   496: dup
    //   497: aload 11
    //   499: invokespecial 56	java/io/IOException:<init>	(Ljava/lang/Throwable;)V
    //   502: athrow
    //   503: ldc 101
    //   505: astore 11
    //   507: sipush 200
    //   510: istore 12
    //   512: iload 5
    //   514: ifeq +12 -> 526
    //   517: ldc 102
    //   519: astore 11
    //   521: sipush 400
    //   524: istore 12
    //   526: aload 4
    //   528: iload 12
    //   530: invokeinterface 103 2 0
    //   535: aload_2
    //   536: iconst_1
    //   537: invokevirtual 104	org/eclipse/jetty/server/Request:setHandled	(Z)V
    //   540: aload 4
    //   542: invokeinterface 105 1 0
    //   547: astore 13
    //   549: aload 13
    //   551: aload 11
    //   553: invokevirtual 106	java/lang/String:getBytes	()[B
    //   556: invokevirtual 107	java/io/OutputStream:write	([B)V
    //   559: aload 13
    //   561: invokevirtual 108	java/io/OutputStream:close	()V
    //   564: goto +8 -> 572
    //   567: astore 13
    //   569: aload 13
    //   571: athrow
    //   572: aload_0
    //   573: getfield 4	com/bloom/proc/HTTPRequestHandler:logger	Lorg/apache/log4j/Logger;
    //   576: invokevirtual 46	org/apache/log4j/Logger:isDebugEnabled	()Z
    //   579: ifeq +138 -> 717
    //   582: invokestatic 45	java/lang/System:nanoTime	()J
    //   585: lstore 13
    //   587: lload 13
    //   589: lload 7
    //   591: lsub
    //   592: lstore 15
    //   594: getstatic 109	com/bloom/proc/HTTPRequestHandler:requestTime	Ljava/util/concurrent/atomic/AtomicLong;
    //   597: lload 15
    //   599: invokevirtual 110	java/util/concurrent/atomic/AtomicLong:addAndGet	(J)J
    //   602: lstore 17
    //   604: getstatic 111	com/bloom/proc/HTTPRequestHandler:requestCount	Ljava/util/concurrent/atomic/AtomicInteger;
    //   607: iconst_1
    //   608: invokevirtual 112	java/util/concurrent/atomic/AtomicInteger:addAndGet	(I)I
    //   611: istore 19
    //   613: lload 13
    //   615: getstatic 113	com/bloom/proc/HTTPRequestHandler:lastOutTime	Ljava/util/concurrent/atomic/AtomicLong;
    //   618: invokevirtual 114	java/util/concurrent/atomic/AtomicLong:get	()J
    //   621: lsub
    //   622: ldc2_w 115
    //   625: ldiv
    //   626: ldc2_w 117
    //   629: lcmp
    //   630: ifle +87 -> 717
    //   633: iload 19
    //   635: getstatic 119	com/bloom/proc/HTTPRequestHandler:lastRequestCount	Ljava/util/concurrent/atomic/AtomicInteger;
    //   638: iload 19
    //   640: invokevirtual 120	java/util/concurrent/atomic/AtomicInteger:getAndSet	(I)I
    //   643: isub
    //   644: istore 20
    //   646: getstatic 113	com/bloom/proc/HTTPRequestHandler:lastOutTime	Ljava/util/concurrent/atomic/AtomicLong;
    //   649: lload 13
    //   651: invokevirtual 121	java/util/concurrent/atomic/AtomicLong:set	(J)V
    //   654: getstatic 109	com/bloom/proc/HTTPRequestHandler:requestTime	Ljava/util/concurrent/atomic/AtomicLong;
    //   657: lconst_0
    //   658: invokevirtual 121	java/util/concurrent/atomic/AtomicLong:set	(J)V
    //   661: aload_0
    //   662: getfield 4	com/bloom/proc/HTTPRequestHandler:logger	Lorg/apache/log4j/Logger;
    //   665: new 36	java/lang/StringBuilder
    //   668: dup
    //   669: invokespecial 37	java/lang/StringBuilder:<init>	()V
    //   672: ldc 122
    //   674: invokevirtual 39	java/lang/StringBuilder:append	(Ljava/lang/String;)Ljava/lang/StringBuilder;
    //   677: iload 20
    //   679: invokevirtual 123	java/lang/StringBuilder:append	(I)Ljava/lang/StringBuilder;
    //   682: ldc 124
    //   684: invokevirtual 39	java/lang/StringBuilder:append	(Ljava/lang/String;)Ljava/lang/StringBuilder;
    //   687: ldc 125
    //   689: invokevirtual 39	java/lang/StringBuilder:append	(Ljava/lang/String;)Ljava/lang/StringBuilder;
    //   692: lload 17
    //   694: l2d
    //   695: ldc2_w 126
    //   698: iload 20
    //   700: i2d
    //   701: dmul
    //   702: ddiv
    //   703: invokevirtual 128	java/lang/StringBuilder:append	(D)Ljava/lang/StringBuilder;
    //   706: ldc -127
    //   708: invokevirtual 39	java/lang/StringBuilder:append	(Ljava/lang/String;)Ljava/lang/StringBuilder;
    //   711: invokevirtual 43	java/lang/StringBuilder:toString	()Ljava/lang/String;
    //   714: invokevirtual 44	org/apache/log4j/Logger:debug	(Ljava/lang/Object;)V
    //   717: return
    // Line number table:
    //   Java source line #357	-> byte code offset #0
    //   Java source line #359	-> byte code offset #3
    //   Java source line #361	-> byte code offset #8
    //   Java source line #362	-> byte code offset #18
    //   Java source line #363	-> byte code offset #52
    //   Java source line #364	-> byte code offset #61
    //   Java source line #368	-> byte code offset #66
    //   Java source line #369	-> byte code offset #73
    //   Java source line #370	-> byte code offset #81
    //   Java source line #371	-> byte code offset #88
    //   Java source line #374	-> byte code offset #93
    //   Java source line #372	-> byte code offset #96
    //   Java source line #373	-> byte code offset #98
    //   Java source line #376	-> byte code offset #108
    //   Java source line #379	-> byte code offset #114
    //   Java source line #389	-> byte code offset #123
    //   Java source line #390	-> byte code offset #137
    //   Java source line #391	-> byte code offset #151
    //   Java source line #392	-> byte code offset #168
    //   Java source line #393	-> byte code offset #214
    //   Java source line #394	-> byte code offset #244
    //   Java source line #395	-> byte code offset #260
    //   Java source line #396	-> byte code offset #275
    //   Java source line #397	-> byte code offset #289
    //   Java source line #398	-> byte code offset #305
    //   Java source line #399	-> byte code offset #320
    //   Java source line #400	-> byte code offset #333
    //   Java source line #403	-> byte code offset #336
    //   Java source line #404	-> byte code offset #346
    //   Java source line #405	-> byte code offset #358
    //   Java source line #407	-> byte code offset #372
    //   Java source line #408	-> byte code offset #377
    //   Java source line #410	-> byte code offset #383
    //   Java source line #411	-> byte code offset #393
    //   Java source line #412	-> byte code offset #402
    //   Java source line #415	-> byte code offset #405
    //   Java source line #428	-> byte code offset #411
    //   Java source line #416	-> byte code offset #414
    //   Java source line #417	-> byte code offset #416
    //   Java source line #418	-> byte code offset #423
    //   Java source line #419	-> byte code offset #431
    //   Java source line #420	-> byte code offset #438
    //   Java source line #421	-> byte code offset #449
    //   Java source line #424	-> byte code offset #452
    //   Java source line #425	-> byte code offset #462
    //   Java source line #426	-> byte code offset #473
    //   Java source line #427	-> byte code offset #476
    //   Java source line #430	-> byte code offset #479
    //   Java source line #433	-> byte code offset #488
    //   Java source line #431	-> byte code offset #491
    //   Java source line #432	-> byte code offset #493
    //   Java source line #435	-> byte code offset #503
    //   Java source line #436	-> byte code offset #507
    //   Java source line #437	-> byte code offset #512
    //   Java source line #438	-> byte code offset #517
    //   Java source line #439	-> byte code offset #521
    //   Java source line #442	-> byte code offset #526
    //   Java source line #443	-> byte code offset #535
    //   Java source line #444	-> byte code offset #540
    //   Java source line #445	-> byte code offset #549
    //   Java source line #446	-> byte code offset #559
    //   Java source line #450	-> byte code offset #564
    //   Java source line #448	-> byte code offset #567
    //   Java source line #449	-> byte code offset #569
    //   Java source line #453	-> byte code offset #572
    //   Java source line #454	-> byte code offset #582
    //   Java source line #455	-> byte code offset #587
    //   Java source line #457	-> byte code offset #594
    //   Java source line #458	-> byte code offset #604
    //   Java source line #460	-> byte code offset #613
    //   Java source line #461	-> byte code offset #633
    //   Java source line #462	-> byte code offset #646
    //   Java source line #463	-> byte code offset #654
    //   Java source line #464	-> byte code offset #661
    //   Java source line #467	-> byte code offset #717
    // Local variable table:
    //   start	length	slot	name	signature
    //   0	718	0	this	HTTPRequestHandler
    //   0	718	1	target	String
    //   0	718	2	baseRequest	Request
    //   0	718	3	request	javax.servlet.http.HttpServletRequest
    //   0	718	4	response	javax.servlet.http.HttpServletResponse
    //   1	512	5	isFailed	boolean
    //   356	7	6	event	Event
    //   6	584	7	stime	long
    //   96	7	9	e1	AdapterException
    //   112	156	9	inputStream	java.io.InputStream
    //   303	22	10	streamReader	com.bloom.source.lib.reader.Reader
    //   121	161	11	eventMetadataMap	Map<String, Object>
    //   491	7	11	e	Exception
    //   505	47	11	responseToBeSent	String
    //   331	16	12	itr	java.util.Iterator<Event>
    //   510	19	12	responseCode	int
    //   334	45	13	isFirstNoRecord	boolean
    //   547	13	13	os	java.io.OutputStream
    //   567	3	13	e	java.io.IOException
    //   585	65	13	etime	long
    //   414	55	14	exp	RuntimeException
    //   421	11	15	t	Throwable
    //   592	6	15	diff	long
    //   436	3	16	rExp	com.bloom.common.exc.RecordException
    //   602	91	17	ttime	long
    //   611	28	19	count	int
    //   644	55	20	cdiff	int
    // Exception table:
    //   from	to	target	type
    //   66	93	96	com/bloom/common/exc/AdapterException
    //   336	402	414	java/lang/RuntimeException
    //   405	411	414	java/lang/RuntimeException
    //   114	488	491	java/lang/Exception
    //   526	564	567	java/io/IOException
  }
  
  private void extractHeaders(Request header)
    throws AdapterException
  {
    this.contentType = header.getHeader("Content-Type");
    this.encoding = header.getHeader("Accept-Encoding");
    this.charset = header.getHeader("Accept-Charset");
    populatePropertyMap();
  }
  
  private void loadParserDefaultPropertyValues(String parserName)
    throws AdapterException
  {
    try
    {
      String[] namespaceAndName = splitNamespaceAndName(parserName, EntityType.PROPERTYTEMPLATE);
      MetaInfo.PropertyTemplateInfo parserPropertyTemplate = (MetaInfo.PropertyTemplateInfo)MetadataRepository.getINSTANCE().getMetaObjectByName(EntityType.PROPERTYTEMPLATE, namespaceAndName[0], namespaceAndName[1], null, WASecurityManager.TOKEN);
      Map<String, MetaInfo.PropertyDef> tps = parserPropertyTemplate.getPropertyMap();
      for (Map.Entry<String, MetaInfo.PropertyDef> tp : tps.entrySet())
      {
        String pname = (String)tp.getKey();
        MetaInfo.PropertyDef def = (MetaInfo.PropertyDef)tp.getValue();
        Class<?> ptype = def.type;
        String defVal = def.defaultValue;
        Object val = null;
        val = castDefaultValueToProperType(defVal, ptype);
        this.propertyMap.put(pname, val);
      }
    }
    catch (SecurityException|MetaDataRepositoryException e)
    {
      AdapterException se = new AdapterException(Error.GENERIC_EXCEPTION, e);
      throw se;
    }
  }
  
  private Object castDefaultValueToProperType(String value, Class<?> type)
  {
    Object correctType = null;
    if (value.isEmpty()) {
      return null;
    }
    if (type == Character.class)
    {
      correctType = Character.valueOf(value.charAt(0));
    }
    else if (type == Boolean.class)
    {
      if ((value.equalsIgnoreCase("true")) || (value.equalsIgnoreCase("yes"))) {
        correctType = Boolean.valueOf(true);
      } else {
        correctType = Boolean.valueOf(false);
      }
    }
    else if (type == Password.class)
    {
      correctType = new Password();
      ((Password)correctType).setPlain(value);
    }
    else
    {
      try
      {
        if (type.isPrimitive()) {
          type = CompilerUtils.getBoxingType(type);
        }
        correctType = type.getConstructor(new Class[] { String.class }).newInstance(new Object[] { value });
      }
      catch (InstantiationException|IllegalAccessException|IllegalArgumentException|InvocationTargetException|NoSuchMethodException|SecurityException e)
      {
        this.logger.error(e.getMessage());
      }
    }
    return correctType;
  }
  
  private String[] splitNamespaceAndName(String name, EntityType type)
  {
    MetaInfo.Namespace curNamespace = MetaInfo.GlobalNamespace;
    String[] namespaceAndName = new String[2];
    if (name.indexOf('.') == -1)
    {
      if (EntityType.isGlobal(type)) {
        namespaceAndName[0] = "Global";
      } else {
        namespaceAndName[0] = curNamespace.name;
      }
      namespaceAndName[1] = name;
    }
    else
    {
      namespaceAndName[0] = name.split("\\.")[0];
      namespaceAndName[1] = name.split("\\.")[1];
    }
    return namespaceAndName;
  }
  
  private void extractURI(String query)
    throws AdapterException
  {
    if (query != null)
    {
      this.propertyMap.put("nvprecorddelimiter", "&");
      this.propertyMap.put("nvpvaluedelimiter", "=");
      NameValueParser nvp = new NameValueParser(new NVPProperty(this.propertyMap));
      Map<String, String> queryMap = nvp.convertToMap(query);
      this.contentType = ((String)queryMap.get("type"));
      this.encoding = ((String)queryMap.get("encoding"));
      this.charset = ((String)queryMap.get("charset"));
      this.uriParserName = ((String)queryMap.get("parser"));
      queryMap.remove("type");
      queryMap.remove("encoding");
      queryMap.remove("parser");
      queryMap.remove("charset");
      populatePropertyMap();
      this.propertyMap.putAll(queryMap);
    }
    else
    {
      this.extractHeader = true;
    }
  }
  
  private void populatePropertyMap()
    throws AdapterException
  {
    this.propertyMap.put(this.COMPRESSION_TYPE, this.encoding);
    this.propertyMap.put("charset", this.charset);
    if (this.uriParserName != null) {
      this.propertyMap.put("handler", getParserName(this.uriParserName));
    } else {
      this.propertyMap.put("handler", getParserName(this.contentType));
    }
    String parserName = (String)this.propertyMap.get("handler");
    loadParserDefaultPropertyValues(parserName);
  }
  
  private String getParserName(String type)
    throws AdapterException
  {
    type = type.toLowerCase();
    if (type.equals("json")) {
      return "JSONParser";
    }
    if ((type.equals("csv")) || (type.equals("dsv"))) {
      return "DSVParser";
    }
    if (type.equals("xml")) {
      return "XMLParser";
    }
    AdapterException se = new AdapterException("Unsupported parser");
    throw se;
  }
}
