package com.postgressive.client;
package events;

import scala.collection.mutable.{Publisher,Subscriber};
import scala.concurrent.{Future,Promise,promise};
import messages.NoticeField;
import io.netty.channel.ChannelHandlerContext;

class Event(private[client] val context:ChannelHandlerContext);

class ConnectionComplete(implicit context:ChannelHandlerContext) extends Event(context);
class ConnectionFailed(val reason:Throwable)(implicit context:ChannelHandlerContext) extends Event(context);
class ConnectionReady(implicit context:ChannelHandlerContext) extends Event(context);
class ConnectionClosed(implicit context:ChannelHandlerContext) extends Event(context);
class MD5AuthenticationDataNeeded(val salt:Array[Byte])(implicit context:ChannelHandlerContext) extends Event(context);
class ClearPasswordNeeded(implicit context:ChannelHandlerContext) extends Event(context);
class AuthenticationRejected(val reason:Throwable)(implicit context:ChannelHandlerContext) extends Event(context);
class AuthenticationCompleted(implicit context:ChannelHandlerContext) extends Event(context);
class NoticeEvent(val data:NoticeField)(implicit context:ChannelHandlerContext) extends Event(context);
class BackendKeyDataEvent(val pid:Int, val key:Int)(implicit context:ChannelHandlerContext) extends Event(context);
class ParameterStatusEvent(val param:String, val value:String)(implicit context:ChannelHandlerContext) extends Event(context);
class NotificationEvent(val pid:Int, val channelName:String, val payload:String)(implicit context:ChannelHandlerContext) extends Event(context);
class ParseCompleteEvent(implicit context:ChannelHandlerContext) extends Event(context);
