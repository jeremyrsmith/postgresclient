package com.postgressive.client.messages


import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import scala.List;

object MsgTypes {
  val AuthMsg:Byte = 'R';
  val KeyMsg:Byte = 'K';
  val BindMsg:Byte = 'B';
  val BindCompleteMsg:Byte = '2';
  val CloseMsg:Byte = 'C';
  val CloseCompleteMsg:Byte = '3';
  val CommandCompleteMsg:Byte = 'C';
  val CopyDataMsg:Byte = 'd';
  val CopyDoneMsg:Byte = 'c';
  val CopyFailMsg:Byte = 'f';
  val CopyInResponseMsg:Byte = 'G';
  val CopyOutResponseMsg:Byte = 'H';
  val CopyBothResponseMsg:Byte = 'W';
  val DataRowMsg:Byte = 'D';
  val DescribeMsg:Byte = 'D';
  val EmptyQueryResponseMsg:Byte = 'I';
  val ErrorResponseMsg:Byte = 'E';
  val ExecuteMsg:Byte = 'E';
  val FlushMsg:Byte = 'H';
  val FunctionCallMsg:Byte = 'F';
  val FunctionCallResponseMsg:Byte = 'V';
  val NoDataMsg:Byte = 'n';
  val NoticeResponseMsg:Byte = 'N';
  val NotificationResponseMsg:Byte = 'A';
  val ParameterDescriptionMsg:Byte = 't';
  val ParameterStatusMsg:Byte = 'S';
  val ParseMsg:Byte = 'P';
  val ParseCompleteMsg:Byte = '1';
  val PasswordMsg:Byte = 'p';
  val PortalSuspendedMsg:Byte = 's';
  val QueryMsg:Byte = 'Q';
  val ReadyForQueryMsg:Byte = 'Z';
  val RowDescriptionMsg:Byte = 'T';
  val SyncMsg:Byte = 'S';
  val TerminateMsg:Byte = 'X';
  val SSLRequestMsg:Byte = 0x08;
}

object AuthMsgTypes {
  val AuthenticationOk = 0;
  val AuthenticationKerberosV5 = 2;
  val AuthenticationCleartextPassword = 3;
  val AuthenticationMD5Password = 5;
  val AuthenticationSCMCredential = 6;
  val AuthenticationGSS = 7;
  val AuthenticationSSPI = 9;
  val AuthenticationGSSContinue = 8;
  
}

class WonkyShitMessage extends PostgresMessage;

class PgBinaryDecoder(clientCharset:java.nio.charset.Charset = null) extends ByteToMessageDecoder {
  
  private var charset:java.nio.charset.Charset = clientCharset match {
    case null => java.nio.charset.Charset.forName("UTF-8");
    case something:java.nio.charset.Charset => something ;
  }
  
  protected def decode(ctx: ChannelHandlerContext, buf: ByteBuf, out: java.util.List[Object]): Unit = { 
	val  channel = ctx.channel();
	
    val bufferLength = buf.readableBytes();
    
    if(bufferLength >= 5) {	//need at least 5 bytes to determine type & length of message
    	
    	val msgType = buf.readByte();
    	
    	//save the readerIndex of the thing
    	val initialReaderIndex = buf.readerIndex();
    	val msgLength = buf.readInt();
    	
    	
    	if(bufferLength >= msgLength) {
    	
			val len = msgLength - 4;	//already read 4 byte msgLength integer
			val ret = msgType match {
	    	  case MsgTypes.AuthMsg => handleAuthMsg(ctx, channel, buf, len);
	    	  case MsgTypes.KeyMsg => handleKeyMsg(ctx, channel, buf, len);
	    	  case MsgTypes.BindCompleteMsg => handleBindCompleteMsg(ctx, channel, buf, len);
	    	  case MsgTypes.CloseCompleteMsg => handleCloseCompleteMsg(ctx, channel, buf, len);
	    	  case MsgTypes.CommandCompleteMsg => handleCommandCompleteMsg(ctx, channel, buf, len);
	    	  case MsgTypes.CopyDataMsg => handleCopyDataMsg(ctx, channel, buf, len);
	    	  case MsgTypes.CopyDoneMsg => handleCopyDoneMsg(ctx, channel, buf, len);
	    	  case MsgTypes.CopyInResponseMsg => handleCopyInResponseMsg(ctx, channel, buf, len);
	    	  case MsgTypes.CopyOutResponseMsg => handleCopyOutResponseMsg(ctx, channel, buf, len);
	    	  case MsgTypes.CopyBothResponseMsg => handleCopyBothResponseMsg(ctx, channel, buf, len);
	    	  case MsgTypes.DataRowMsg => handleDataRowMsg(ctx, channel, buf, len);
	    	  case MsgTypes.EmptyQueryResponseMsg => handleEmptyQueryResponseMsg(ctx, channel, buf, len);
	    	  case MsgTypes.ErrorResponseMsg => handleErrorResponseMsg(ctx, channel, buf, len);
	    	  case MsgTypes.FunctionCallResponseMsg => handleFunctionCallResponseMsg(ctx, channel, buf, len);
	    	  case MsgTypes.NoDataMsg => handleNoDataMsg(ctx, channel, buf, len);
	    	  case MsgTypes.NoticeResponseMsg => handleNoticeResponseMsg(ctx, channel, buf, len);
	    	  case MsgTypes.NotificationResponseMsg => handleNotificationResponseMsg(ctx, channel, buf, len);
	    	  case MsgTypes.ParameterDescriptionMsg => handleParameterDescriptionMsg(ctx, channel, buf, len);
	    	  case MsgTypes.ParameterStatusMsg => handleParameterStatusMsg(ctx, channel, buf, len);
	    	  case MsgTypes.ParseCompleteMsg => handleParseCompleteMsg(ctx, channel, buf, len);
	    	  case MsgTypes.PortalSuspendedMsg => handlePortalSuspendedMsg(ctx, channel, buf, len);
	    	  case MsgTypes.ReadyForQueryMsg => handleReadyForQueryMsg(ctx, channel, buf, len);
	    	  case MsgTypes.RowDescriptionMsg => handleRowDescriptionMsg(ctx, channel, buf, len);
	    	  case a => 
	    	 	  println("Unexpected message type: " + a.toChar + " (" + a + ") in message of length " + len);
	    	 	  throw new Exception("Unexpected message type");
			}
			if(buf.readerIndex() < initialReaderIndex + msgLength) {
			  println("For some reason I am having to skip bytes to keep in sync.", ret);
			  buf.skipBytes(msgLength - (buf.readerIndex() - initialReaderIndex));
			}
			//println("Returning a message " + ret.getClass().toString())
			
			out.add(ret);
    	} else {
    		//skip backwards so I can pick up the type and length at the next attempt
    		buf.skipBytes(-5);
    	}
    	 	
	}
    //println("I got some stuff but I don't have enough data yet to form a message");
	
  }
  
  //builds the proper type of AuthenticationMessage
  private def handleAuthMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):AuthenticationMessage = {
    val authMsgType = buf.readInt();
    authMsgType match {
      case AuthMsgTypes.AuthenticationOk => new AuthenticationOk;
      case AuthMsgTypes.AuthenticationKerberosV5 => new AuthenticationKerberosV5;
      case AuthMsgTypes.AuthenticationCleartextPassword => new AuthenticationCleartextPassword;
      case AuthMsgTypes.AuthenticationMD5Password => {
	    val salt = new Array[Byte](4); 
	    buf.readBytes(salt, 0, 4);
	    new AuthenticationMD5Password(salt);
      }
      case AuthMsgTypes.AuthenticationSCMCredential => new AuthenticationSCMCredential;
      case AuthMsgTypes.AuthenticationGSS => new AuthenticationGSS;
      case AuthMsgTypes.AuthenticationSSPI => new AuthenticationSSPI;
      case AuthMsgTypes.AuthenticationGSSContinue => {
    	  // read the rest of the auth data - len - 4 bytes (because we already read the authMsgType which is part of len)
    	  // into the byte array
    	  var data:Array[Byte] = new Array[Byte](len - 4);
    	  for(i <- 0 until (len - 4)) {
    	    data(i) = buf.readByte();
    	  }
    	  new AuthenticationGSSContinue(data);
      }
      case _ => null;
    }
  }
  
  private def handleKeyMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):BackendKeyData = new BackendKeyData(buf.readInt(), buf.readInt());
 
  
  private def handleBindCompleteMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):BindComplete = new BindComplete();
  
  private def handleCloseCompleteMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):CloseComplete = new CloseComplete();
  
  private def handleCommandCompleteMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):CommandComplete = {
		  val str = buf.readBytes(buf.bytesBefore(0.asInstanceOf[Byte])).toString(charset);
		  buf.readByte(); //read zero terminator
		  new CommandComplete(str);
  }
  
  private def handleCopyDataMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int) = {
    var data = new Array[Byte](len);
    for(i <- 0 until len) {
      data(i) = buf.readByte();
    }
    new CopyData(data);
  }
  
  private def handleCopyDoneMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int) = new CopyDone();
  
  private def handleCopyInResponseMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int) = {
    val format = buf.readByte();
    val numColumns = buf.readShort();
    val formatCodes = new Array[Short](numColumns);
    for(i <- 0 until numColumns) {
      formatCodes(i) = buf.readShort();
    }
    new CopyInResponse(format, numColumns, formatCodes)
  }
  
  private def handleCopyOutResponseMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int) = {
    val format = buf.readByte();
    val numColumns = buf.readShort();
    val formatCodes = new Array[Short](numColumns);
    for(i <- 0 until numColumns) {
      formatCodes(i) = buf.readShort();
    }
    new CopyOutResponse(format, numColumns, formatCodes)
  }
  
  private def handleCopyBothResponseMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):CopyBothResponse = {
    val format = buf.readByte();
    val numColumns = buf.readShort();
    val formatCodes = new Array[Short](numColumns);
    for(i <- 0 until numColumns) {
      formatCodes(i) = buf.readShort();
    }
    new CopyBothResponse(format, numColumns, formatCodes)
  }
  
  private def handleDataRowMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):DataRow = {
    println("Data row message of length " + len + " with " + buf.readableBytes() + " available");
	val numColumns = buf.readShort();
    new DataRow(numColumns, numColumns match {
      case 0 => new Array[DataRowColumn](0);
      case otherValue => {
    	var tmp = new Array[DataRowColumn](otherValue);
    	for(i <- 0 until otherValue) {
    	  val dataLen = buf.readInt();
    	  dataLen match {
    	    case -1 | 0 => tmp(i) = new DataRowColumn(dataLen, Unpooled.buffer(0));
    	    case _ => {
	    	  val data = buf.readBytes(dataLen);
	    	  tmp(i) = new DataRowColumn(dataLen,data);
    	    }
    	  }
    	}
    	tmp;
      }
    });
    
  }
  
  private def handleEmptyQueryResponseMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):EmptyQueryResponse = {
	//the entire message has already been read
    new EmptyQueryResponse;
  }
  
  private def handleErrorResponseMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):ErrorResponse = {
    
    var read:Int = 0;
  	var idx:Int = 0;
  	var fields:List[ErrorField] = List[ErrorField]();
    while(read < len - 1) {
      val fieldType = buf.readByte();
      val strLen = buf.bytesBefore(0.asInstanceOf[Byte]);
      val str = buf.readBytes(strLen).toString(charset);
      fields = new ErrorField(fieldType, str) :: fields;
      buf.skipBytes(1);	//discard null terminator
      read += strLen + 2;	//field type + string + null terminator
    }
    buf.skipBytes(1); //final null terminator
    new ErrorResponse(fields);
    
  }
  
  private def handleFunctionCallResponseMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):FunctionCallResponse = {
    val dataLen = buf.readInt();
    new FunctionCallResponse(dataLen, buf.readBytes(dataLen));
  }
  
  private def handleNoDataMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):NoData = new NoData();
  
  private def handleNoticeResponseMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):NoticeResponse = {
    var read:Int = 0;
  	var idx:Int = 0;
  	var fields:List[NoticeField] = List[NoticeField]();
    while(read < len - 1) {
      val fieldType = buf.readByte();
      val strLen = buf.bytesBefore(0.asInstanceOf[Byte]);
      val str = buf.readBytes(strLen).toString(charset);
      fields = new NoticeField(fieldType, str) :: fields;
      buf.skipBytes(1);	//discard null terminator
      read += strLen + 2;	//field type + string + null terminator
    }
    new NoticeResponse(fields);
  }
  
  private def handleNotificationResponseMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):NotificationResponse = {
    val pid = buf.readInt();
    val channelName = buf.readBytes(buf.bytesBefore(0.asInstanceOf[Byte])).toString(charset);
    buf.skipBytes(1);	//discard null terminator
    val payload = buf.readBytes(buf.bytesBefore(0.asInstanceOf[Byte])).toString(charset);
    buf.skipBytes(1);	//discard null terminator
    new NotificationResponse(pid, channelName, payload);
  }
  
  private def handleParameterDescriptionMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):ParameterDescription = {
    val numFields = buf.readShort();
    val fieldOids = new Array[Int](numFields);
    for(i <- 0 until numFields) {
      fieldOids(i) = buf.readInt();
    }
    new ParameterDescription(numFields, fieldOids);
  }
  
  private def handleParameterStatusMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):ParameterStatus = {
    val parameterName = buf.readBytes(buf.bytesBefore(0.asInstanceOf[Byte])).toString(charset);
    buf.skipBytes(1);	//discard null terminator
    val parameterValue = buf.readBytes(buf.bytesBefore(0.asInstanceOf[Byte])).toString(charset);
    buf.skipBytes(1);
    new ParameterStatus(parameterName, parameterValue);
  }
  
  private def handleParseCompleteMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int) = new ParseComplete();
  
  private def handlePortalSuspendedMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int) = new PortalSuspended();
  
  private def handleReadyForQueryMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int) = new ReadyForQuery(buf.readByte());
  
  private def handleRowDescriptionMsg(ctx: ChannelHandlerContext, channel: Channel, buf: ByteBuf, len:Int):RowDescription = {
    val numFields = buf.readShort();
    val columns = new Array[ColumnDescriptor](numFields);
    for(i <- 0 until numFields) {
    	val fieldName = buf.readBytes(buf.bytesBefore(0.asInstanceOf[Byte])).toString(charset);
    	buf.skipBytes(1); //discard null terminator
    	columns(i) = new ColumnDescriptor(fieldName,buf.readInt(), buf.readShort(), buf.readInt(), buf.readShort(), buf.readInt(), buf.readShort());
    }
    new RowDescription(numFields, columns);
  }
  

}