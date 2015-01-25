package com.postgressive.client;
package query;

import scala.concurrent.{Promise,Future};
import scala.collection.mutable.{Publisher,Subscriber,ListBuffer};


abstract class Query[A](val query:String)(implicit m: Manifest[A]) {
	
	def resultClass = m.runtimeClass;
	def arity:Int;
	
	
	def begin(conn:PostgresConnection);
}

class QueryWithoutResults(query:String) extends Query[Unit](query) {
	def arity = 0;
	
	val queryPromise = Promise[Unit];
	
	private[client] def completed() = queryPromise.success(Unit);
	private[client] def failed(cause:Throwable) = queryPromise.failure(cause)
	
}

/*class RowAggregator[A <: Product] extends Subscriber[Publisher[messages.DataRow], messages.DataRow] {
	val rows = new ListBuffer[A];
	
	
}*/

abstract class QueryWithResults[A <: Product](query:String)(implicit m:Manifest[A]) extends Query[A](query) {
	
	val cls = m.runtimeClass;
	val ctor = cls.getConstructors()(0);
	val members = ctor.getParameterTypes() zip ctor.getGenericParameterTypes();
	val arity = members.length;
	
	val queryPromise = Promise[List[A]]();
	
	private[client] def completed(value:List[A]) = queryPromise.success(value);
	private[client] def failed(cause:Throwable) = queryPromise.failure(cause);
	
	override def begin(conn:PostgresConnection) = {
		//TODO: begin listening for RowMessage events and aggregate them
	}
	
	// TODO: both a map((List[A]) => Unit) and take(Int, (List[A]) => Unit) API
	
}



class TestFinder {
	def findIt[A <: Product] (a:A)(implicit m:Manifest[A]) = {
		val clz = m.runtimeClass;
		val ctor = clz.getConstructors()(0);
		val params = ctor.getGenericParameterTypes();
		
		params map {
			p => println(p);
		}
	}
}
