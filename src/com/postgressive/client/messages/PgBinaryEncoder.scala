package com.postgressive.client.messages

import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.channel.Channel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.Charset;

class PgBinaryEncoder(clientCharset:Charset = null) extends MessageToMessageEncoder[PostgresMessage] {

  var charset:Charset = clientCharset match {
    case null => Charset.forName("UTF-8");
    case c:Charset => c;
  };
  
  protected def encode(ctx: ChannelHandlerContext, msg: PostgresMessage, out:java.util.List[Object]): Unit = { 
    msg match {
      case pgMsg:PostgresFrontendMessage => {
        val buf = Unpooled.directBuffer(pgMsg.length());
        pgMsg.write(buf);
        out.add(buf);
      };
      case _ => ;
    }
  }

}