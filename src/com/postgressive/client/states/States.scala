package com.postgressive.client;
package states;

import messages._;
import events._;
import exceptions._;
import io.netty.channel.ChannelHandlerContext;

abstract class State(val isAvailable:Boolean,val isFailed:Boolean) {
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])];
}

abstract class WaitingState() extends State(false,false);
abstract class FrontendWaitingState() extends State(false,false);
abstract class ErrorState() extends State(false, true);
abstract class ReadyState() extends State(true,false);

case class NotConnected() extends WaitingState { //Not connected yet
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case StartupMessage(a,b,c) => (StartupSent(), Nil); 
	}
}

case class StartupSent() extends WaitingState { //startup message / ssl startup message sent.  Awaiting reply.
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case AuthenticationOk() => (AuthCompleted(), Nil);
		
		//TODO: Support Kerberos Auth
		case AuthenticationKerberosV5() => (AuthNotSupported(), List(new ConnectionFailed(new AuthNotSupportedException("Kerberos Authentication Not Supported Yet."))));
		
		case AuthenticationMD5Password(salt) => (AwaitingMD5Password(), List(new MD5AuthenticationDataNeeded(salt)));
		
		case AuthenticationCleartextPassword() => (AwaitingCleartextPassword(), List(new ClearPasswordNeeded()));
		
		//TODO: Support SCM Auth
		case AuthenticationSCMCredential() => (AuthNotSupported(), List(new ConnectionFailed(new AuthNotSupportedException("SCM Authentication Not Supported Yet."))));
		
		//TODO: Support GSS Auth
		case AuthenticationGSS() => (AuthNotSupported(), List(new ConnectionFailed(new AuthNotSupportedException("GSS Authentication Not Supported Yet."))));
		case AuthenticationGSSContinue(_) => (AuthNotSupported(), List(new ConnectionFailed(new AuthNotSupportedException("GSS Authentication Not Supported Yet."))));
			
		//TODO: Support SCM Auth
		case AuthenticationSSPI() => (AuthNotSupported(), List(new ConnectionFailed(new AuthNotSupportedException("SSPI Authentication Not Supported Yet."))));
		
		case msg:ErrorResponse => (AuthRejected(), List(new ConnectionFailed(new AuthenticationRejectedException(msg.message, msg.code))));
	}
}

case class AuthNotSupported() extends ErrorState { //server is demanding an auth method I don't support.  Entering this state implies closing the connection
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case _ => (NotConnected(), Nil);
	}
}

case class AwaitingMD5Password() extends FrontendWaitingState {
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case PasswordMessage(_) => (AuthMD5Dialog(), Nil);
	}
}

case class AwaitingCleartextPassword() extends FrontendWaitingState {
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case PasswordMessage(_) => (AuthPlainDialog(), Nil);
	}
}

case class AuthRejected() extends ErrorState { //we were rejected by the server.
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case _ => (NotConnected(), Nil);
	}
}

case class StartupFailed() extends ErrorState { //the startup phase errored for some other reason.
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case _ => (NotConnected(), Nil);
	}
}

case class AuthMD5Dialog() extends WaitingState { //sent an MD5 password hash; waiting for response
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case AuthenticationOk() => (AuthCompleted(), List(new AuthenticationCompleted()));
		case NoticeResponse(fields) => (AuthMD5Dialog(), fields map (new NoticeEvent(_)));
		case msg:ErrorResponse => (AuthRejected(), List(new ConnectionFailed(new AuthenticationRejectedException(msg.message, msg.code))));
	}
}

case class AuthPlainDialog() extends WaitingState {	//sent a plaintext password; waiting for response
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case AuthenticationOk() => (AuthCompleted(), List(new AuthenticationCompleted()));
		case NoticeResponse(fields) => (AuthMD5Dialog(), fields map (new NoticeEvent(_)));
		case msg:ErrorResponse => (AuthRejected(), List(new ConnectionFailed(new AuthenticationRejectedException(msg.message, msg.code))));
	}
}


case class AuthCompleted() extends WaitingState { //succesfully authorized, awaiting further messages
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case ReadyForQuery(_) => (Ready(), List(new ConnectionReady()));
		case NoticeResponse(fields) => (AuthCompleted(), fields map (new NoticeEvent(_)));
		case BackendKeyData(pid,key) => (AuthCompleted(), List(new BackendKeyDataEvent(pid,key)));
		case ParameterStatus(param,value) => (AuthCompleted(), List(new ParameterStatusEvent(param,value)));
	}
}

case class Ready() extends ReadyState { //ready for a query
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case Parse(_,_,_,_) => (WaitingForParse(), Nil);	//front end message, so no event needed
		case Bind(_,_,_,_,_,_) => (WaitingForBind(), Nil);	//can bind without doing parse (prepared statement)
		case NotificationResponse(pid,channelName,payload) => (Ready(), List(new NotificationEvent(pid, channelName, payload)));
		case NoticeResponse(fields) => (Ready(), fields map (new NoticeEvent(_)));
	}
}

case class QueryInProcess() extends FrontendWaitingState {
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case Bind(_,_,_,_,_,_) => (WaitingForBind(), Nil);
	}
}

case class WaitingForParse() extends WaitingState {
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case Bind(_,_,_,_,_,_) => (WaitingForBind(), Nil);
		case Flush() => (WaitingForParse(), Nil);
		case ParseComplete() => (QueryInProcess(), List(new ParseCompleteEvent));
		
	}
}

case class WaitingForBind() extends WaitingState {
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case _:PostgresMessage => (this, Nil);
	}
}

case class WaitingForDescription() extends WaitingState {
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case _:PostgresMessage => (this, Nil);
	}
}

case class WaitingForExecution() extends WaitingState {
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case _:PostgresMessage => (this, Nil);
	}
}

case class CopyIn() extends WaitingState {
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case _:PostgresMessage => (this, Nil);
	}
}

case class CopyOut() extends WaitingState {
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case _:PostgresMessage => (this, Nil);
	}
}

case class Syncing() extends WaitingState {
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case _:PostgresMessage => (this, Nil);
	}
}

case class Busy() extends WaitingState { //a query has been sent but the response has not been fully received yet
	def message(implicit ctx:ChannelHandlerContext):PartialFunction[PostgresMessage,(State,List[Event])] = {
		case _:PostgresMessage => (this, Nil);
	}
}