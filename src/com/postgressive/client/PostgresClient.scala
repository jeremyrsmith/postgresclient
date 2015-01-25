package com.postgressive.client;
import query.Query;

import scala.concurrent.{Future,Promise};
import scala.collection.mutable.{Queue,Publisher,Subscriber};

import states._;
import query.Query;
import messages._
import events._;

import java.net.SocketAddress;
import java.nio.charset.Charset;


import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.{ChannelInitializer,ChannelHandlerAdapter,ChannelHandlerContext,ChannelPromise,ChannelFuture,ChannelFutureListener};
import io.netty.bootstrap.Bootstrap;

object PostgresClient {

	def connect(
		host: SocketAddress,
		user: String,
		dbname: String = null,
		password: String = null,
		charset: Charset = Charset.forName("UTF-8"))(implicit ec:scala.concurrent.ExecutionContext):Future[PostgresConnection] = {
		
		val connectionPromise = Promise[PostgresConnection]();
		
		val conn = PostgresConnection.create(host, user, dbname, password, charset);
		
		//inject password from here (to avoid storing it in the connection)
		conn.events.md5AuthenticationDataNeeded.future map {
			event => conn.supplyMD5Credential(password, event.salt , event.context)
		}
		conn.events.clearPasswordNeeded.future map {
			event => conn.supplyPlainCredential(password, event.context)
		}
		
		
		//map success or failure to the promise
		conn.events.connectionFailed.future map {
			event => connectionPromise.failure(event.reason)
		}
		
		conn.events.connectionReady.future map {
			_ =>
				connectionPromise.success(conn)
		}
		
		connectionPromise.future
	}
	
	
}

private object PostgresConnection {
	def create (host: SocketAddress,
		user: String,
		dbname: String = null,
		password: String = null,
		charset: Charset = Charset.forName("UTF-8")) = new PostgresConnection(host, user, dbname, charset);
	
	class StateMachine extends Subscriber[PostgresMessage,Publisher[PostgresMessage]] with Publisher[events.Event]  {
		private var state:State = new NotConnected();
		
		override def notify(handler:Publisher[PostgresMessage], msg:PostgresMessage) = {
			implicit val ctx = msg.context.get;
			try {
				state.message(ctx)(msg) match {
			
					case (newState, events) =>
						//publish emitted events
						events foreach {
							case e => publish(e);
						}
						
						//transition state
						state = newState;
				}
			} catch {
				case err:Throwable =>
					//TODO: handle
					println("Invalid message type " + msg.getClass().getName() + " for state " + state.getClass().getName());
			}
		}
	}
}

class PostgresConnection private (host: SocketAddress,
		user: String,
		dbname: String = null,
		charset: Charset = Charset.forName("UTF-8")) {
	
	val events = new Events;
	
	private val stateMachine:PostgresConnection.StateMachine = new PostgresConnection.StateMachine;
	private val queryQueue = new Queue[Query[_]]();
	private val handler = PostgresHandler.create;
	
	//receives backend messages from the handler
	handler.subscribe(stateMachine);
	
	//receives events from state machine
	stateMachine.subscribe(new Subscriber[Event,Publisher[Event]] {
		override def notify(pub: Publisher[Event], event: Event) = events.dispatch(event);
	});
	
	private val connection = new Bootstrap();
	connection.group( new NioEventLoopGroup() )
		.channel( classOf[NioSocketChannel] )
		.handler( new ChannelInitializer[SocketChannel]() {
				override def initChannel( ch: SocketChannel ): Unit = {
					val pipeline = ch.pipeline();
					pipeline.addLast( "decoder", new PgBinaryDecoder( charset ) );
					pipeline.addLast( "encoder", new PgBinaryEncoder( charset ) );
					pipeline.addLast( "handler", handler );
				
				}
			} 
		);
	
	private[client] def beginQuery[A <: Product](query:Query[A]) = {
		
	}
	
	def hexDigest( input: Array[Byte] ): String = {
		val d = java.security.MessageDigest.getInstance( "MD5" );
		val bytes = d.digest( input );
		val num = new java.math.BigInteger( 1, bytes );
		String.format( "%0" + ( bytes.length << 1 ) + "x", num );
	}
	
	//calculate MD5 message and write to channel
	private[client] def supplyMD5Credential(password:String, salt:Array[Byte], ctx:ChannelHandlerContext) = {
		ctx write PasswordMessage(( "md5" + hexDigest( hexDigest( ( password + user ).getBytes() ).getBytes() ++ salt ) ).getBytes())
	}
	
	//write plain auth message to channel
	private[client] def supplyPlainCredential(password:String, ctx:ChannelHandlerContext) = {
		ctx write PasswordMessage(password.getBytes() ++ Array[Byte](0))
	}
	
	
	private val connectionFuture = connection.connect(host);
	
	connectionFuture.addListener(new ChannelFutureListener {
		def operationComplete( future: ChannelFuture ): Unit = {
			if(future.isSuccess()) {
				//write startup message to bootstrap things
				future.channel write StartupMessage(user, dbname, charset);
				future.channel flush;
				
				//all future messages should be handled by the state machine and query
				
			}
		}
	})
	
	
	//this is an inner class so that dispatch() can be private to PostgresConnection.
	class Events {
		
		
		val anyEvent = new EventPublisher[Event]
		val connectionComplete = new EventPublisher[ConnectionComplete]
		val connectionFailed = new EventPublisher[ConnectionFailed]
		val connectionReady = new EventPublisher[ConnectionReady]
		val connectionClosed = new EventPublisher[ConnectionClosed]
		val md5AuthenticationDataNeeded = new EventPublisher[MD5AuthenticationDataNeeded]
		val clearPasswordNeeded = new EventPublisher[ClearPasswordNeeded]
		val authenticationRejected = new EventPublisher[AuthenticationRejected]
		val authenticationCompleted = new EventPublisher[AuthenticationCompleted]
		val noticeEvent = new EventPublisher[NoticeEvent]
		val backendKeyDataEvent = new EventPublisher[BackendKeyDataEvent]
		val parameterStatusEvent = new EventPublisher[ParameterStatusEvent]
		val notificationEvent = new EventPublisher[NotificationEvent]
		val parseCompleteEvent = new EventPublisher[ParseCompleteEvent]
		
		private[PostgresConnection] def dispatch(event:Event) {
			event match {
				case e:ConnectionComplete	=> connectionComplete.dispatch(e);
				case e:ConnectionFailed		=> connectionFailed.dispatch(e);
				case e:ConnectionReady		=> connectionReady.dispatch(e);
				case e:ConnectionClosed		=> connectionClosed.dispatch(e);
				case e:MD5AuthenticationDataNeeded => md5AuthenticationDataNeeded.dispatch(e);
				case e:ClearPasswordNeeded => clearPasswordNeeded.dispatch(e);
				case e:AuthenticationRejected => authenticationRejected.dispatch(e);
				case e:AuthenticationCompleted => authenticationCompleted.dispatch(e);
				case e:NoticeEvent => noticeEvent.dispatch(e);
				case e:BackendKeyDataEvent => backendKeyDataEvent.dispatch(e);
				case e:NotificationEvent => notificationEvent.dispatch(e);
				case e:ParseCompleteEvent => parseCompleteEvent.dispatch(e);
			}
			anyEvent.dispatch(event);
		}
		
		class EventPublisher[A <: Event] extends Publisher[A] {
			private[Events] def dispatch(event:A) = {
				publish(event);
			}
			
			/*
			 * TODO: must track futures so that they can be failed when the event publisher is destroyed.
			 * Otherwise there will be a memory leak from contexts waiting on a future that will never complete.
			 */
			
			def future(): Future[A] = {
				val p = Promise[A]();
				val sub = new Subscriber[A,Publisher[A]] {
					override def notify(sub:Publisher[A], e:A) = {
						sub.removeSubscription(this);
						p.success(e);
					}
				}
				p.future;
			}
		}
	}
}

private object PostgresHandler {
	def create = new PostgresHandler;
}

class PostgresHandler private extends ChannelHandlerAdapter with Publisher[PostgresMessage] {
	
	override def channelRead(ctx:ChannelHandlerContext, msg:Object) = {
		msg match {
			case msg:PostgresBackendMessage =>
				publish(msg withContext ctx);
		}
	}
	
	override def write(ctx:ChannelHandlerContext, msg:Object, p:ChannelPromise) = {
		msg match {
			case msg:PostgresFrontendMessage => 
				super.write(ctx,msg,p);
				publish(msg withContext ctx);
				
			case msg:PostgresBackendMessage =>
				p.setFailure(new exceptions.InvalidMessageException("A backend message should not be written to the connection."));
			case _ =>
				p.setFailure(new exceptions.InvalidMessageException("Only a PostgresFrontendMessage can be written to the connection."));
		}
	}
	
	
}
