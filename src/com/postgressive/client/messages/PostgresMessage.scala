package com.postgressive.client.messages
import io.netty.channel.ChannelHandlerContext

class PostgresMessage(clientCharset:java.nio.charset.Charset = null) {
	var charset:java.nio.charset.Charset = clientCharset match {
	  case null => java.nio.charset.Charset.forName("UTF-8");
	  case something:java.nio.charset.Charset => something;
	}
	
	implicit var context:Option[ChannelHandlerContext] = None;
	
	def withContext(c:ChannelHandlerContext) = {
		context = Some(c);
		this
	}
}