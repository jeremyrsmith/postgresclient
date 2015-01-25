package com.postgressive.client;
package types;

import io.netty.buffer.ByteBuf;
import scala.collection.mutable.HashMap;
import java.nio.charset.Charset;
import messages.{UntypedDataRowColumn,TypedDataRowColumn,RowDescription,DataRow};
import scala.collection.mutable.ArrayLike;
import language.implicitConversions;
import java.util.Calendar;


import scala.reflect.runtime._;

import scala.math._;


abstract class PgVal[+T](value:T) {
  def length(encoding:Charset):Int;
  def write(buf:ByteBuf, encoding:Charset);
  def oid(implicit conn:PostgresConnection):Int;
  def value():T = value;
  override def toString() = value.toString();
  
}

object PgVal {	//implicit conversions
  implicit def PgValToScalaEquiv[T](value:PgVal[T]):T = value.value();
  implicit def ShortToPgInt2(value:Short):Int2 = new Int2(value);
  implicit def IntToPgInt4(value:Int):Int4 = new Int4(value);
  implicit def LongToPgInt8(value:Long):Int8 = new Int8(value);
  implicit def BooleanToPgBool(value:Boolean):Bool = new Bool(value);
  implicit def ByteArrayToPgBytea(value:Array[Byte]) = new Bytea(value);
  implicit def StringToPgText(value:String):Text = new Text(value);
  implicit def XmlToPgXml(value:scala.xml.Node):Xml = new Xml(value);
  implicit def FloatToFloat4(value:Float):Float4 = new Float4(value);
  implicit def DoubleToFloat8(value:Double):Float8 = new Float8(value);
  implicit def StringToUuid(value:String):Uuid = new Uuid(value);
  implicit def DateToTimestamp(value:java.util.Date) = new Timestamp(value);
}

class PgArray[A <: Option[PgVal[_]]](values:Array[A], val arrayOid:Int, val elemOid:Int) extends PgVal[Array[A]](values) {
  def apply(i:Integer) = values(i);
  def foreach[U](f:(A => U)):Unit = values.foreach(f);
  
  def length(encoding:Charset) = values.foldLeft[Int](0)((current:Int, item:A) => item match {
    case Some(item) => current + item.length(encoding);
  	case _ => current + 4;	//4 bytes for -1 (signaling null)
  })
  
  def toArray = values.clone;
  
  def write(buf:ByteBuf, encoding:Charset) {
    
  }
  override def oid(implicit conn:PostgresConnection) = arrayOid;
  override def toString() = values.toString();
  
}

/* built-in OIDs which are hard coded into postgres itself */
object PostgresOids {
  val BoolOid = 16;
  val ByteaOid = 17;
  val CharOid = 18;
  val NameOid = 19;
  val Int8Oid = 20;
  val Int2Oid = 21;
  val Int2VectorOid = 22;
  val Int4Oid = 23;
  val TextOid = 25;
  val OidOid = 26;
  val TidOid = 27;
  val XidOid = 28;
  val OidVectorOid = 30;
  val XmlOid = 142;
  val PointOid = 600;
  val LineSegOid = 601;
  val PathOid = 602;
  val BoxOid = 603;
  val PolygonOid = 604;
  val Float4Oid = 700;
  val Float8Oid = 701;
  val AbstimeOid = 702;
  val ReltimeOid = 704;
  val TintervalOid = 705;
  val CircleOid = 718;
  val MoneyOid = 790;
  val MacaddrOid = 829;
  val InetOid = 869;
  val CidrOid = 650;
  val BoolArrayOid = 1000;
  val ByteaArrayOid = 1001;
  val CharArrayOid = 1002;
  val NameArrayOid = 1003;
  val Int2ArrayOid = 1005;
  val Int2VectorArrayOid = 1006;
  val Int4ArrayOid = 1007;
  val TextArrayOid = 1009;
  val TidArrayOid = 1010;
  val XidArrayOid = 1011;
  val CidArrayOid = 1012;
  val OidVectorArrayOid = 1013;
  val BpCharArrayOid = 1014;
  val VarcharArrayOid = 1015;
  
  val OidArrayOid = 1028;
  
  
  val BpcharOid = 1042;
  val VarcharOid = 1043;
  val DateOid =  1082;
  val TimestampOid = 1114;
  val UuidOid = 2950;
}

import PostgresOids._;

object Decoders {
  def boolrecv (raw:UntypedDataRowColumn,charset:Charset) = {
	raw.dataLength match {
      case 1 => Some(new Bool(raw.data.readByte() != 0));
      case _ => None;
    }
  };
  
  def bytearecv (raw:UntypedDataRowColumn,charset:Charset) = {
    raw.dataLength match {
      case -1 => None;
      case len => {
        val result = new Array[Byte](len);
        raw.data.readBytes(result, 0, len);
        Some(new Bytea(result));
      }
    }
  };
  
  def charrecv (raw:UntypedDataRowColumn,charset:Charset) = {
    raw.dataLength match {
      case -1 => None;
      case a => {
        val result = new Array[Byte](a);
        raw.data.readBytes(result, 0, a);
        Some(new PgChar(new String(result, charset)));
      }
    }
  };
  
  def namerecv (raw:UntypedDataRowColumn,charset:Charset) = {
    raw.dataLength match {
      case -1 => None;
      case a => {
        val result = new Array[Byte](a);
        raw.data.readBytes(result, 0, a);
        Some(new Name(new String(result, charset)));
      }
    }
  };
  
  def int8recv (raw:UntypedDataRowColumn,charset:Charset) = {
	raw.dataLength match {
      case 8 => Some(new Int8(raw.data.readLong()));
      case _ => None;
	}
  };
  
  def int2recv (raw:UntypedDataRowColumn,charset:Charset) = {
	raw.dataLength match {
      case 2 => Some(new Int4(raw.data.readShort()));
      case _ => None;
	}
  }
  
  def int2vectorrecv (raw:UntypedDataRowColumn, charset:Charset) = {
    raw.dataLength match {
      case -1 => None;
      case a => Some(ValDecoder.decodeArray[Int2](raw, charset));
    }
  };
  
  def int4recv (raw:UntypedDataRowColumn,charset:Charset) = {
	raw.dataLength match {
      case 4 => Some(new Int4(raw.data.readInt()));
      case _ => None;
	}
  };
  
  def textrecv (raw:UntypedDataRowColumn, charset:Charset) = {
    raw.dataLength match {
      case -1 => None;
      case a => {
        val result = raw.data.array;
        Some(new Text(new String(result, charset)));
      }
    }
  };
  
  def oidrecv (raw:UntypedDataRowColumn, charset:Charset) = {
    raw.dataLength match {
      case 4 => Some(new Oid(raw.data.readInt()));
      case _ => None;
    }
  };
  
  def xml_recv (raw:UntypedDataRowColumn, charset:Charset) = {
    raw.dataLength match {
      case -1 => None;
      case len => Some(new Xml(scala.xml.XML.loadString(raw.data.readBytes(len).toString(charset))));
    }
  };
  
  def uuid_recv (raw:UntypedDataRowColumn, charset:Charset) = {
    //get bytes
    raw.dataLength match {
    	case -1 => None;
    	case 16 => Some(new Uuid(raw.data.readBytes(16).array().map("%02X" format _).mkString));
    	case len => println("Unexpected UUID length " + len); println(new String(raw.data.readBytes(len).array(), charset)); None;
    }
    
  }
  
}

object ValDecoder {
  
  def decodeArray[A <: PgVal[_]](raw:UntypedDataRowColumn, charset:Charset)(implicit mA:Manifest[A]) = {
    val buf = raw.data;
    val ndim = buf.readInt();
    //TODO: some validation?
    val dims = new Array[Int](ndim);
    val lBound = new Array[Int](ndim);
    val flags = buf.readInt();
    val elementType = buf.readInt();
    
    for(i <- 0 until ndim) {
      dims(i) = buf.readInt();
      lBound(i) = buf.readInt();
      if(dims(i) != 0) {
        val ub = lBound(i) + dims(i) - 1;
        if(lBound(i) > ub) {
          //TODO: this is some kind of error condition
        }
      }
    }
    
    //TODO: backend does this calculation as a long to detect overflow and throw error.  Should I?
    val nitems = dims.reduceLeft((current:Int,dim:Int) => { current * dim });
    val items = new Array[Option[A]](nitems);
    var hasnull = false;
    for(i <- 0 until nitems) {
      val itemlen = buf.readInt();
      items(i) = itemlen match {
        case -1 => None;
        case len => decodeAny[A](new TypedDataRowColumn[A](elementType, itemlen, buf.readSlice(itemlen)), charset);
      }
    }
    
    new PgArray[Option[A]](items, raw.oid, elementType);
  }
  
  private var decoders:HashMap[Int,(UntypedDataRowColumn,Charset) => Option[PgVal[_]]] = HashMap(
	  BoolOid -> Decoders.boolrecv,
	  ByteaOid -> Decoders.bytearecv,
	  CharOid -> Decoders.charrecv,
	  NameOid -> Decoders.namerecv,
	  Int8Oid -> Decoders.int8recv,
	  Int2Oid -> Decoders.int2recv,
	  Int2VectorOid -> Decoders.int2vectorrecv,
	  Int4Oid -> Decoders.int4recv,
	  TextOid -> Decoders.textrecv,
	  OidOid -> Decoders.oidrecv,
	  //TidOid ->
	  //XidOid ->
	  //OidVectorOid ->
	  XmlOid -> Decoders.xml_recv,
	  PointOid -> ((raw:UntypedDataRowColumn, charset:Charset) => {
	    raw.dataLength match {
	      case -1 => None;
	      case len => Some(new Point(raw.data.readDouble(), raw.data.readDouble()));
	    }
	  }),
	  LineSegOid -> ((raw:UntypedDataRowColumn, charset:Charset) => {
	    raw.dataLength match {
	      case -1 => None;
	      case 32 => Some(new LineSeg(
	              new Point(raw.data.readDouble(), raw.data.readDouble()), 
	              new Point(raw.data.readDouble(), raw.data.readDouble()))
	      );
	    }
	  }),
	  PathOid -> ((raw:UntypedDataRowColumn, charset:Charset) => {
	    raw.dataLength match {
	      case -1 => None;
	      case len => None;	//TODO: make this work
	    }
	  }),
	  TextArrayOid -> ((raw:UntypedDataRowColumn, charset:Charset) => {
	    raw.dataLength match {
	      case -1 => None;
	      case len => Some(decodeArray[Text](raw, charset));
	    }
	  }),
	  UuidOid -> Decoders.uuid_recv
	  
  );
  
  def addDecoder(oid:Int, d:(UntypedDataRowColumn,Charset) => Option[PgVal[_]]):Unit = {
    decoders.put(oid, d);
  }
  
  def decodeAny[A <: PgVal[_]](raw:TypedDataRowColumn[A], charset:Charset)(implicit mA:Manifest[A]):Option[A] = {
    if(decoders.contains(raw.oid)) {
      	decoders.get(raw.oid) match {
      	  case None => None;
      	  case Some(decoder) => decoder(raw, charset).asInstanceOf[Option[A]];  
      	}
    } else {
        println("couldn't find decoder for oid " + raw.oid);
        None;
    }    
  };
  
  def decodeAny(raw:UntypedDataRowColumn, charset:Charset) = {
    if(decoders.contains(raw.oid)) {
      	decoders.get(raw.oid) match {
      	  case None => None;
      	  case Some(decoder) => decoder(raw, charset);  
      	}
    } else {
        println("couldn't find decoder for oid " + raw.oid);
        None;
    }   
  }
  
  //TODO: waiting for 2.10 to be finalized
  /*
  def decodeValObject[A](msg:DataRow, charset:Charset)(implicit mA:Manifest[A], description:RowDescription):A = {
    val rootMirror = universe.runtimeMirror(mA.erasure.getClassLoader)
    val t = universe.typeOf[A];
    t.members.find(_.isMethod).map(_.asMethodSymbol).find(_.isConstructor)
    val typ = rootMirror.reflectClass(rootMirror.classSymbol(mA.erasure)).;
   
  }*/
  
  //attempts to decode a result row as an object of type A
  //by decoding each field and attempting to assign it to the corresponding field (by name) of the class A.
  def decodeObject[A](msg:DataRow, charset:Charset)(implicit mA:Manifest[A], description:RowDescription):A = {
    val typ = mA.erasure;							//TODO: update this to ConcreteTypeTag
    val obj = typ.newInstance.asInstanceOf[A];		//result object
    val methods = typ.getMethods();					//all methods in the class
    
    msg.columns.zip(description.fields).foreach(arg => {	//zip the data together with the field descriptors to iterate simultaneously
      val columnDescriptor = arg._2;
      val columnData = arg._1;
      try {
        
        //find the setter method for the field
    	val m = methods.find(_.getName == columnDescriptor.fieldName + "_$eq");
    	
    	val p = (m match {
    	  case None => None;
    	  case Some(method) => {
    	    //setter method was found, prepare to decode the field data
    	    val raw = new TypedDataRowColumn[PgVal[Any]](columnDescriptor.typeOid, columnData.dataLength, columnData.data);
    	    
    	    //make sure the setter method takes 1 parameter of the proper type
    	    method.getParameterTypes.length match {
    	      case 1 => {
    	    
    	    	//decode the data
	    	    val decoded = decoders.contains(raw.oid) match {
	    	    	case true =>
				      	decoders.get(raw.oid) match {
				      	  case None => None;
				      	  case Some(decoder) => decoder(raw, charset);
				      	}
	    	    	case false =>
	    	    	  None;
		    	};
		    	
		    	decoded match {
		    	  case None => ;
		    	  case Some(value) => {
		    		//if the setter method takes the type we have decoded to, invoke it
		    	    method.getParameterTypes()(0).isAssignableFrom(value.getClass()) match {
		    	      case true => {
		    	        val args = List[Object](value);
		    	        method.invoke(obj, args: _*);
		    	      }
		    	      case false => ;
		    	    }
		    	  }
		    	}
    	    	
    	      }
    	      case _ => None;
    	    }
    	  }
    	});
      } catch { case err:Throwable =>
       // println(err);
        err.printStackTrace();
      }
    });
    
    obj;
  }
  
}

class Bool(value:Boolean) extends PgVal[Boolean](value) {
  override def length(encoding:Charset) = 1;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeByte(value match { 
    case true => 1;
    case false => 0;
  });
  override def oid(implicit conn:PostgresConnection) = 16;
}

class Bytea(value:Array[Byte]) extends PgVal[Array[Byte]](value) {
  override def length(encoding:Charset) = value.length;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeBytes(value);
  override def oid(implicit conn:PostgresConnection) = 17;
}

class PgChar(value:String) extends PgVal[String](value) {
  override def length(encoding:Charset) = value.getBytes(encoding).length;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeBytes(value.getBytes(encoding));
  override def oid(implicit conn:PostgresConnection) = 18;
}

object PgChar {
  implicit def Char2PgChar(v:Char):PgChar = new PgChar(v.toString());
}

class Name(value:String) extends PgVal[String](value) {
  override def length(encoding:Charset) = value.getBytes(encoding).length;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeBytes(value.getBytes(encoding));
  override def oid(implicit conn:PostgresConnection) = 19;
  override def toString = value;
}

class Int8(value:Long) extends PgVal[Long](value) {
  override def length(encoding:Charset) = 8;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeLong(value);
  override def oid(implicit conn:PostgresConnection) = 20;
}

class Int2(value:Short) extends PgVal[Short](value) {
  override def length(encoding:Charset) = 2;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeShort(value);  
  override def oid(implicit conn:PostgresConnection) = 21;
}

class Int2Vector(value:Array[Short]) extends PgVal[Array[Short]](value) {
  override def length(encoding:Charset) = value.length * 2;
  override def write(buf:ByteBuf, encoding:Charset) = value.foreach(item => buf.writeShort(item));
  override def oid(implicit conn:PostgresConnection) = 22;
}

class Int4(value:Int) extends PgVal[Int](value) {
  override def length(encoding:Charset) = 4;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeInt(value);
  override def oid(implicit conn:PostgresConnection) = 23;
}

//omitted: 24 => regproc

class Text(value:String) extends PgVal[String](value) {
  override def length(encoding:Charset) = value.getBytes(encoding).length;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeBytes(value.getBytes(encoding));
  override def oid(implicit conn:PostgresConnection) = 25;
  
  override def toString() = value;
}

class Oid(value:Int) extends Int4(value) {
  override def oid(implicit conn:PostgresConnection) = 26;
}

class Tid(value:Tuple2[Int,Int]) extends PgVal[Tuple2[Int,Int]](value) {
  override def length(encoding:Charset) = 8;
  override def write(buf:ByteBuf, encoding:Charset) = {
    buf.writeInt(value._1);
    buf.writeInt(value._2);
  }
  override def oid(implicit conn:PostgresConnection)=27;
}

class Xid(value:Int) extends Int4(value) {
  override def oid(implicit conn:PostgresConnection) = 28;
}

//omitted: cid

class OidVector(value:Array[Int]) extends PgVal[Array[Int]](value) {
  override def length(encoding:Charset) = value.length * 4;
  override def write(buf:ByteBuf, encoding:Charset) = value.foreach(item => buf.writeInt(item));
  override def oid(implicit conn:PostgresConnection) = 30;
}

//TODO: System catalog types?

class Xml(value:scala.xml.Node) extends PgVal[scala.xml.Node](value) {
  override def length(encoding:Charset) = value.toString().getBytes(encoding).length;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeBytes(value.toString().getBytes(encoding));
  override def oid(implicit conn:PostgresConnection) = 142;
}


class Point(value:Tuple2[Double,Double]) extends PgVal[Tuple2[Double,Double]](value) {
  override def length(encoding:Charset) = 16;
  override def write(buf:ByteBuf, encoding:Charset) = { buf.writeDouble(value._1); buf.writeDouble(value._2); }
  override def oid(implicit conn:PostgresConnection) = 600;
  
}

object Point {
  def apply(x:Double, y:Double) = new Point(x,y);
}

//TODO: useful things in these classes

class LineSeg(value:Tuple2[Point,Point]) extends PgVal[Tuple2[Point,Point]](value) {
  override def length(encoding:Charset) = 32;
  override def write(buf:ByteBuf, encoding:Charset) = {
    buf.writeDouble(value._1._1);
    buf.writeDouble(value._1._2);
    buf.writeDouble(value._2._1);
    buf.writeDouble(value._2._2);
  }
  override def oid(implicit conn:PostgresConnection) = 601;
}

class Path(value:List[Point], val closed:Boolean) extends PgVal[List[Point]](value) {
  override def length(encoding:Charset) = value.length * 16 + 1;
  override def write(buf:ByteBuf, encoding:Charset) = {
    buf.writeByte(closed match { case true => 1; case false => 0;});
    value.foreach(item => {buf.writeDouble(item.value._1); buf.writeDouble(item.value._2);});
  }
  override def oid(implicit conn:PostgresConnection) = 602;
}

class Box(value:Tuple2[Point,Point]) extends PgVal[Tuple2[Point,Point]](value) {
  override def length(encoding:Charset) = 32;
  override def write(buf:ByteBuf, encoding:Charset) = {
    buf.writeDouble(value._1._1);
    buf.writeDouble(value._1._2);
    buf.writeDouble(value._2._1);
    buf.writeDouble(value._2._2);
  }
  override def oid(implicit conn:PostgresConnection) = 603;
}


class Polygon(value:List[Point]) extends PgVal[List[Point]](value) {
  override def length(encoding:Charset) = value.length * 16;
  override def write(buf:ByteBuf, encoding:Charset) = {
    value.foreach(item => {buf.writeDouble(item._1); buf.writeDouble(item._2);});
  }
  override def oid(implicit conn:PostgresConnection) = 604;
}

//omitted: Line

class Float4(value:Float) extends PgVal[Float](value) {
  override def length(encoding:Charset) = 4;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeFloat(value);
  override def oid(implicit conn:PostgresConnection) = 700;
}

class Float8(value:Double) extends PgVal[Double](value) {
  override def length(encoding:Charset) = 8;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeDouble(value);
  override def oid(implicit conn:PostgresConnection) = 701;
}

class Abstime(value:java.util.Date) extends PgVal[java.util.Date](value) {
  override def length(encoding:Charset) = 4;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeInt(value.getTime().toInt / 1000);
  override def oid(implicit conn:PostgresConnection) = 702;
}

class Reltime(value:Int) extends PgVal[Int](value) {
  override def length(encoding:Charset) = 4;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeInt(value);
  override def oid(implicit conn:PostgresConnection) = 703;
}

class Tinterval(value:Tuple2[Abstime,Abstime]) extends PgVal[Tuple2[Abstime,Abstime]](value) {
  override def length(encoding:Charset) = 8;
  override def write(buf:ByteBuf, encoding:Charset) = {value._1.write(buf,encoding); value._2.write(buf,encoding); }
  override def oid(implicit conn:PostgresConnection) = 704;
}

//omitted: Unknown

class Circle(value:Tuple2[Point,Double]) extends PgVal[Tuple2[Point,Double]](value) {
  override def length(encoding:Charset) = 24;
  override def write(buf:ByteBuf, encoding:Charset) = {
    buf.writeDouble(value._1._1);
    buf.writeDouble(value._1._2);
    buf.writeDouble(value._2);
  }
  override def oid(implicit conn:PostgresConnection) = 718;
}

//this is bad, but we are going to go ahead and assume that fracdigits is 2.
//There may be a country that uses a different precision for their money, but AFAIK postgres makes the same assumption
class Money(value:BigDecimal) extends PgVal[BigDecimal](value) {
  override def length(encoding:Charset) = 8;
  override def write(buf:ByteBuf, encoding:Charset) = {
    val tmp = value.setScale(2);
    buf.writeLong((tmp * 100).toLong);
  }
  override def oid(implicit conn:PostgresConnection) = 790;
}


class Maccaddr(value:Tuple6[Byte,Byte,Byte,Byte,Byte,Byte]) extends PgVal[Tuple6[Byte,Byte,Byte,Byte,Byte,Byte]](value) {
  override def length(encoding:Charset) = 6;
  override def write(buf:ByteBuf, encoding:Charset) = {
    buf.writeByte(value._1);
    buf.writeByte(value._2);
    buf.writeByte(value._3);
    buf.writeByte(value._4);
    buf.writeByte(value._5);
    buf.writeByte(value._6);
  }
  override def oid(implicit conn:PostgresConnection) = 829;
  override def toString() = String.format("%02x:%02x:%02x:%02x:%02x:%02x", value._1:java.lang.Byte, value._2:java.lang.Byte, value._3:java.lang.Byte, value._4:java.lang.Byte, value._5:java.lang.Byte, value._6:java.lang.Byte);
}

class Inet(value:java.net.InetAddress) extends PgVal[java.net.InetAddress](value) {
  override def length(encoding:Charset) = value match {
    case inet4:java.net.Inet4Address => 7;
    case inet6:java.net.Inet6Address => 19;
  };
  override def write(buf:ByteBuf, encoding:Charset) = {
    //write address family magic number
    buf.writeByte(value match {
      case inet4:java.net.Inet4Address => 2;
      case inet6:java.net.Inet6Address => 3;
    });
    //write mask number (32 or 128)
    buf.writeByte(value match {
      case inet4:java.net.Inet4Address => 32;
      case inet6:java.net.Inet6Address => 128;
    });
    //write zero byte to indicate inet instead of cidr
    buf.writeByte(0);
    
    buf.writeBytes(value.getAddress());
  }
  override def oid(implicit conn:PostgresConnection) = 869;
}

class CIDRBlock(val address:java.net.InetAddress, val mask:Byte) {
  //TODO: useful things
}

class Cidr(value:CIDRBlock) extends PgVal[CIDRBlock](value) {
  override def length(encoding:Charset) = value.address match {
    case inet4:java.net.Inet4Address => 7;
    case inet6:java.net.Inet6Address => 19;
  };
  override def write(buf:ByteBuf, encoding:Charset) = {
    //write address family magic number
    buf.writeByte(value.address match {
      case inet4:java.net.Inet4Address => 2;
      case inet6:java.net.Inet6Address => 3;
    });
    //write mask number
    buf.writeByte(value.mask);
    //write one byte to indicate cidr instead of inet
    buf.writeByte(1);
    
    buf.writeBytes(value.address.getAddress());
  }
  override def oid(implicit conn:PostgresConnection) = 650;
}

//blank-padded character data.  In other words char(n).  Same as text for our purposes here
class Bpchar(value:String) extends PgVal[String](value) {
  override def length(encoding:Charset) = value.getBytes(encoding).length;
  override def write(buf:ByteBuf, encoding:Charset) = buf.writeBytes(value.getBytes(encoding));
  override def oid(implicit conn:PostgresConnection) = 1042;
}

class Varchar(value:String) extends Text(value) {
  override def oid(implicit conn:PostgresConnection) = 1043;
}

class Date(value:java.util.Date) extends PgVal[java.util.Date](value) {
  override def length(encoding:Charset) = 4;
  override def write(buf:ByteBuf, encoding:Charset) = {
    val epoch = 946713600000l;	//postgres epoch is Jan 1 2000
    val timeDiff = (value.getTime() - epoch) / 86400000;
    buf.writeInt(timeDiff.toInt);
  }
  override def oid(implicit conn:PostgresConnection) = 1082;
}

class Timestamp(value:java.util.Date) extends PgVal[java.util.Date](value) {
  override def length(encoding:Charset) = 8;
  override def write(buf:ByteBuf, encoding:Charset) = {
    val epoch = 946713600000l;
    val timeDiff = (value.getTime() - epoch) * 1000l;
    buf.writeLong(timeDiff);
  }
  override def oid(implicit conn:PostgresConnection) = 1114;
}

class Uuid(value:String) extends PgVal[String](value) {
  override def length(encoding:Charset) = 16;
  def bytes = org.apache.commons.codec.binary.Hex.decodeHex(value.toCharArray())
  override def write(buf:ByteBuf, encoding:Charset) = {
    val bytes = this.bytes;
    buf.writeBytes(bytes);
  }
  override def oid(implicit conn:PostgresConnection) = UuidOid;
  
  def base64 = org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString(this.bytes);
  def hex() = { 
	  val bytes = this.bytes;
	  org.apache.commons.codec.binary.Hex.encodeHexString(this.bytes)
  }
  
}

class PgNull[A <: PgVal[_]](value:A) extends PgVal[A](value) {
	override def length(encoding:Charset) = -1;
	override def oid(implicit conn:PostgresConnection) = value.oid;
	override def write(buf:ByteBuf, encoding:Charset) = {}
}


//TODO:
//class Time(value:java.util.Date) extends PgVal[java.util.Date]

class Hstore(value:HashMap[String,String]) extends PgVal[HashMap[String,String]](value) {
  
  //four bytes for the number of pairs starts the fold.  The map yields 
  //  +4 bytes for each key length, 
  //  +length of the key, 
  //  +4 bytes for the value length
  //  +length of the value (if non-null)
  override def length(encoding:Charset) = value.map (arg => (arg._1.getBytes(encoding).length + 4) + (4 + arg._2 match { case null => 0; case s:String => s.getBytes(encoding).length })).foldLeft(4)((a,b) => a+b);
  
  override def write(buf:ByteBuf, encoding:Charset) = {
    buf.writeInt(value.size);
    value.foreach(arg => {
      val key = arg._1.getBytes(encoding);
      buf.writeInt(key.length);
      buf.writeBytes(key);
      val value = arg._2 match { case null => null; case s:String => s.getBytes(encoding); }
      buf.writeInt(value match { case null => -1; case a:Array[Byte] => a.length });
      value match { 
        case null => ;
        case a:Array[Byte] => buf.writeBytes(a);
      }
    });
  }
  
  override def oid(implicit conn:PostgresConnection) = 0
}
