package com.postgressive.client;
package messages;


import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;
import types._;
import io.netty.channel.ChannelHandlerContext;

trait PostgresBackendMessage extends PostgresMessage {

}

class AuthenticationMessage extends PostgresBackendMessage {
  
}

case class AuthenticationOk() extends AuthenticationMessage {
  
}

case class AuthenticationKerberosV5() extends AuthenticationMessage {
  
}

case class AuthenticationCleartextPassword() extends AuthenticationMessage {
  
}


case class AuthenticationMD5Password(val salt:Array[Byte]) extends AuthenticationMessage {}

case class AuthenticationSCMCredential() extends AuthenticationMessage {}

case class AuthenticationGSS() extends AuthenticationMessage {}

case class AuthenticationSSPI() extends AuthenticationMessage {}

case class AuthenticationGSSContinue(authData:Array[Byte]) extends AuthenticationMessage {}

case class BackendKeyData(pid:Int, key:Int) extends PostgresBackendMessage {}

case class BindComplete() extends PostgresBackendMessage {}

case class CloseComplete() extends PostgresBackendMessage {}

case class CommandComplete(commandTag:String) extends PostgresBackendMessage {}

case class CopyData(data:Array[Byte]) extends PostgresBackendMessage with PostgresFrontendMessage {
  
  def length() = 5 + data.length;
  
  def write(buf:ByteBuf) = {
    buf.writeByte('d');
    buf.writeInt(4 + data.length);
    buf.writeBytes(data);
  }
}

case class CopyDone() extends PostgresBackendMessage with PostgresFrontendMessage {
  
  def length() = 5;
  
  def write(buf:ByteBuf) = {
    buf.writeByte('c');
    buf.writeInt(4);
  }
}

case class CopyInResponse(format:Byte, numColumns:Short, columnFormats:Array[Short]) extends PostgresBackendMessage {}

case class CopyOutResponse(format:Byte, numColumns:Short, columnFormats:Array[Short]) extends PostgresBackendMessage {}

case class CopyBothResponse(format:Byte, numColumns:Short, columnFormats:Array[Short]) extends PostgresBackendMessage{}

class DataRowColumn(val dataLength:Int, val data:ByteBuf) {}

class UntypedDataRowColumn(val oid:Int, val dataLength:Int, val data:ByteBuf) {
	
}

class TypedDataRowColumn[A <: PgVal[_]](oid:Int, dataLength:Int, data:ByteBuf) extends UntypedDataRowColumn(oid,dataLength,data) {
  
}


case class DataRow(val numColumns:Short, val columns:Array[DataRowColumn]) extends PostgresBackendMessage {}

case class EmptyQueryResponse() extends PostgresBackendMessage {}

class ErrorField(val fieldType:Byte, val fieldValue:String) {}

case class ErrorResponse(fields:List[ErrorField]) extends PostgresBackendMessage {
  
  val severity:String = fields.filter(_.fieldType == 'S').map(_.fieldValue).mkString(";");
  val code:String = fields.filter(_.fieldType == 'C').map(_.fieldValue).mkString(";");;
  val message:String = fields.filter(_.fieldType == 'M').map(_.fieldValue).mkString(";");;
  
  val rawFields = fields.map(f => (f.fieldType.toString, f.fieldValue)).toMap;

}

case class FunctionCallResponse(resultLen:Int, result:ByteBuf) extends PostgresBackendMessage {}

case class NoData() extends PostgresBackendMessage {}

case class NoticeField(fieldType:Byte, fieldValue:String);

case class NoticeResponse(fields:List[NoticeField]) extends PostgresBackendMessage {}

case class NotificationResponse(pid:Int, channelName:String, payload:String) extends PostgresBackendMessage {}

case class ParameterDescription(numParams:Short, paramOids:Array[Int]) extends PostgresBackendMessage {}

case class ParameterStatus(param:String, value:String) extends PostgresBackendMessage {}

case class ParseComplete() extends PostgresBackendMessage {}

case class PortalSuspended() extends PostgresBackendMessage {}

case class ReadyForQuery(val status:Byte) extends PostgresBackendMessage {}

class ColumnDescriptor(val fieldName:String, val tableOid:Int, val tableColumn:Short, val typeOid:Int, val typeLen:Short, val typeMod:Int, val format:Short) {}

case class RowDescription(val numFields:Short, val fields:Array[ColumnDescriptor]) extends PostgresBackendMessage {}

