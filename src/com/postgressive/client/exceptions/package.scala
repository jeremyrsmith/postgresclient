package com.postgressive.client

package object exceptions {

	class InvalidMessageException(msg:String) extends Throwable(msg);
	class AuthNotSupportedException(msg:String) extends Throwable(msg);
	class AuthenticationRejectedException(msg:String, val code:String) extends Throwable(msg);
}