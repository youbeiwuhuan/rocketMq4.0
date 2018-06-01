/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 定义rocketmq通讯报文格式
 * 
 * <pre>
 * 报文的结构：
 * +------------------+-------------------+------------------+--------------------+
 * |    length        |   header length   |   header data    |         body       |
 * +------------------+-------------------+------------------+--------------------+
 * 
 * </pre>
 *
 */
public class RemotingCommand {
	public static final String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
	public static final String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
	public static final String REMOTING_VERSION_KEY = "rocketmq.remoting.version";
	
	private static final Logger log = LoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);
	
	private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND
	private static final int RPC_ONEWAY = 1; // 0, RPC
	/**
	 * 缓存CommandCustomHeader子类的属性，提高反射性能
	 */
	private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP = new HashMap<Class<? extends CommandCustomHeader>, Field[]>();
	/**
	 * 缓存类的标准名称
	 */
	private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<Class, String>();

	// 1, Oneway
	// 1, RESPONSE_COMMAND
	/**
	 * 缓存类属性上{@link CFNotNull}注解配置，下面是基础类和包装类的
	 */
	private static final Map<Field, Annotation> NOT_NULL_ANNOTATION_CACHE = new HashMap<Field, Annotation>();
	private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
	private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
	private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
	private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
	private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
	private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
	private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
	private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
	private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();
	private static volatile int configVersion = -1;
	/**
	 * 请求ID的生成器
	 */
	private static AtomicInteger requestId = new AtomicInteger(0);

	/**
	 * 序列化方式，默认为json。 之所以要搞个json序列化方式，我觉得是为了开发时候调试方便，直接可以从日志看到报文内容，但序列化后报文较大，占用带宽较多。
	 * 实际生产环境还是要严格根据rocketmq的报文协议来序列化
	 * 
	 */
	private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

	static {
		/**
		 * 获取序列化方式
		 */
		final String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
		if (!isBlank(protocol)) {
			try {
				serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
			} catch (IllegalArgumentException e) {
				throw new RuntimeException("parser specified protocol error. protocol=" + protocol, e);
			}
		}
	}

	// ----------------报文头的定义 ---------------------
	/**
	 * //
	 * 用于标示请求类型，{@link org.apache.rocketmq.common.protocol.RequestCode},{@link org.apache.rocketmq.common.protocol.ResponseCode}
	 */
	private int code;
	private LanguageCode language = LanguageCode.JAVA;
	private int version = 0;
	/**
	 * // 每个报文的唯一标志，request和response通过该字段匹配
	 */
	private int opaque = requestId.getAndIncrement();
	private int flag = 0;
	private String remark;
	/**
	 * // 传输时使用，CommandCustomHeader转为该结构<key,value>后，再统一转为json传输。 因此
	 * CommandCustomHeader只能是String,Int,Long等基础数据结构，不能是复合数据结构
	 */
	private HashMap<String, String> extFields;

	/**
	 * 在MQ中，所有数据传输都使用该数据结构进行数据传输，当把数据转为网络传输时，
	 * 会将customHeader转为HashMap的extFields，再转为json串,所以 这里用是transient修饰，
	 * 参看{@link #makeCustomHeaderToNet()}
	 */
	private transient CommandCustomHeader customHeader;
	// ----------------报文头的定义 end---------------------

	private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

	/**
	 * 消息体内容，需要根据报文头来确定报文类型后解析
	 */
	private transient byte[] body;// 为啥是transient?因为序列化是自己实现的不依赖Java序列化

	protected RemotingCommand() {
	}
	
	

	/**
	 * 创建请求的报文
	 * 
	 * @param code
	 *            用于标示请求类型，{@link org.apache.rocketmq.common.protocol.RequestCode},{@link org.apache.rocketmq.common.protocol.ResponseCode}
	 * @param customHeader
	 * @return
	 */
	public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
		RemotingCommand cmd = new RemotingCommand();
		cmd.setCode(code);
		cmd.customHeader = customHeader;
		setCmdVersion(cmd);
		return cmd;
	}

	/**
	 * 设置版本号
	 * @param cmd 要设置版本号的报文
	 */
	private static void setCmdVersion(RemotingCommand cmd) {
		if (configVersion >= 0) {
			cmd.setVersion(configVersion);
		} else {
			String v = System.getProperty(REMOTING_VERSION_KEY);
			if (v != null) {
				int value = Integer.parseInt(v);
				cmd.setVersion(value);
				configVersion = value;
			}
		}
	}

	/**
	 * 创建错误回应的报文
	 * 
	 * @param classHeader 用户请求头的类型
	 * @return
	 */
	public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
		return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
	}

	/**
	 * 创建回应的报文
	 * 
	 * @param code
	 * @param remark
	 * @param classHeader  用户请求头的类型
	 * @return
	 */
	public static RemotingCommand createResponseCommand(int code, String remark,
			Class<? extends CommandCustomHeader> classHeader) {
		RemotingCommand cmd = new RemotingCommand();
		cmd.markResponseType();
		cmd.setCode(code);
		cmd.setRemark(remark);
		setCmdVersion(cmd);

		if (classHeader != null) {
			try {
				CommandCustomHeader objectHeader = classHeader.newInstance();
				cmd.customHeader = objectHeader;
			} catch (InstantiationException e) {
				return null;
			} catch (IllegalAccessException e) {
				return null;
			}
		}

		return cmd;
	}

	/**
	 * 创建回应的报文
	 * 
	 * @param code
	 * @param remark
	 * @return
	 */
	public static RemotingCommand createResponseCommand(int code, String remark) {
		return createResponseCommand(code, remark, null);
	}

	/**
	 * 反序列化
	 * 
	 * @param array
	 * @return
	 */
	public static RemotingCommand decode(final byte[] array) {
		ByteBuffer byteBuffer = ByteBuffer.wrap(array);
		return decode(byteBuffer);
	}

	/**
	 * 反序列化
	 * 
	 * @param byteBuffer
	 * @return
	 */
	public static RemotingCommand decode(final ByteBuffer byteBuffer) {
		int length = byteBuffer.limit();
		int oriHeaderLen = byteBuffer.getInt();
		int headerLength = getHeaderLength(oriHeaderLen);

		byte[] headerData = new byte[headerLength];
		byteBuffer.get(headerData);

		RemotingCommand cmd = headerDecode(headerData, getProtocolType(oriHeaderLen));

		int bodyLength = length - 4 - headerLength;
		byte[] bodyData = null;
		if (bodyLength > 0) {
			bodyData = new byte[bodyLength];
			byteBuffer.get(bodyData);
		}
		cmd.body = bodyData;

		return cmd;
	}

	public static int getHeaderLength(int length) {
		return length & 0xFFFFFF;
	}

	private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
		switch (type) {
		case JSON:
			RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
			resultJson.setSerializeTypeCurrentRPC(type);
			return resultJson;
		case ROCKETMQ:
			RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(headerData);
			resultRMQ.setSerializeTypeCurrentRPC(type);
			return resultRMQ;
		default:
			break;
		}

		return null;
	}

	public static SerializeType getProtocolType(int source) {
		return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
	}

	public static int createNewRequestId() {
		return requestId.incrementAndGet();
	}

	public static SerializeType getSerializeTypeConfigInThisServer() {
		return serializeTypeConfigInThisServer;
	}

	private static boolean isBlank(String str) {
		int strLen;
		if (str == null || (strLen = str.length()) == 0) {
			return true;
		}
		for (int i = 0; i < strLen; i++) {
			if (!Character.isWhitespace(str.charAt(i))) {
				return false;
			}
		}
		return true;
	}

	public static byte[] markProtocolType(int source, SerializeType type) {
		byte[] result = new byte[4];

		result[0] = type.getCode();
		result[1] = (byte) ((source >> 16) & 0xFF);
		result[2] = (byte) ((source >> 8) & 0xFF);
		result[3] = (byte) (source & 0xFF);
		return result;
	}
	
//---------------------------以上全是静态方法，以下全是对象方法-------------------------------------------------------------------------
	

	public void markResponseType() {
		int bits = 1 << RPC_TYPE;
		this.flag |= bits;
	}

	public CommandCustomHeader readCustomHeader() {
		return customHeader;
	}

	public void writeCustomHeader(CommandCustomHeader customHeader) {
		this.customHeader = customHeader;
	}

	public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader)
			throws RemotingCommandException {
		CommandCustomHeader objectHeader;
		try {
			objectHeader = classHeader.newInstance();
		} catch (InstantiationException e) {
			return null;
		} catch (IllegalAccessException e) {
			return null;
		}

		if (this.extFields != null) {

			Field[] fields = getClazzFields(classHeader);
			for (Field field : fields) {
				if (!Modifier.isStatic(field.getModifiers())) {
					String fieldName = field.getName();
					if (!fieldName.startsWith("this")) {
						try {
							String value = this.extFields.get(fieldName);
							if (null == value) {
								Annotation annotation = getNotNullAnnotation(field);
								if (annotation != null) {
									throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
								}

								continue;
							}

							field.setAccessible(true);
							String type = getCanonicalName(field.getType());
							Object valueParsed;

							if (type.equals(STRING_CANONICAL_NAME)) {
								valueParsed = value;
							} else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
								valueParsed = Integer.parseInt(value);
							} else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
								valueParsed = Long.parseLong(value);
							} else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
								valueParsed = Boolean.parseBoolean(value);
							} else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
								valueParsed = Double.parseDouble(value);
							} else {
								throw new RemotingCommandException(
										"the custom field <" + fieldName + "> type is not supported");
							}

							field.set(objectHeader, valueParsed);

						} catch (Throwable e) {
						}
					}
				}
			}

			objectHeader.checkFields();
		}

		return objectHeader;
	}

	private Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
		Field[] field = CLASS_HASH_MAP.get(classHeader);

		if (field == null) {
			field = classHeader.getDeclaredFields();
			synchronized (CLASS_HASH_MAP) {
				CLASS_HASH_MAP.put(classHeader, field);
			}
		}
		return field;
	}

	private Annotation getNotNullAnnotation(Field field) {
		Annotation annotation = NOT_NULL_ANNOTATION_CACHE.get(field);

		if (annotation == null) {
			annotation = field.getAnnotation(CFNotNull.class);
			synchronized (NOT_NULL_ANNOTATION_CACHE) {
				NOT_NULL_ANNOTATION_CACHE.put(field, annotation);
			}
		}
		return annotation;
	}

	/**
	 * 获取类的标准名称
	 * 
	 * @param clazz
	 * @return
	 */
	private String getCanonicalName(Class clazz) {
		String name = CANONICAL_NAME_CACHE.get(clazz);

		if (name == null) {
			name = clazz.getCanonicalName();
			synchronized (CANONICAL_NAME_CACHE) {
				CANONICAL_NAME_CACHE.put(clazz, name);
			}
		}
		return name;
	}

	/**
	 * 序列化
	 * 
	 * @return
	 */
	public ByteBuffer encode() {
		// 1> header length size
		int length = 4;

		// 2> header data length
		byte[] headerData = this.headerEncode();
		length += headerData.length;

		// 3> body data length
		if (this.body != null) {
			length += body.length;
		}

		ByteBuffer result = ByteBuffer.allocate(4 + length);

		// length
		result.putInt(length);

		// header length
		result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

		// header data
		result.put(headerData);

		// body data;
		if (this.body != null) {
			result.put(this.body);
		}

		result.flip();

		return result;
	}

	/**
	 * 消息头的序列化
	 * 
	 * @return
	 */
	private byte[] headerEncode() {
		this.makeCustomHeaderToNet();
		if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
			return RocketMQSerializable.rocketMQProtocolEncode(this);
		} else {
			return RemotingSerializable.encode(this);
		}
	}

	/**
	 * 将customHeader转为HashMap的extFields，再转为json串
	 */
	public void makeCustomHeaderToNet() {
		if (this.customHeader != null) {
			Field[] fields = getClazzFields(customHeader.getClass());
			if (null == this.extFields) {
				this.extFields = new HashMap<String, String>();
			}

			for (Field field : fields) {
				if (!Modifier.isStatic(field.getModifiers())) {
					String name = field.getName();
					if (!name.startsWith("this")) {
						Object value = null;
						try {
							field.setAccessible(true);
							value = field.get(this.customHeader);
						} catch (IllegalArgumentException e) {
						} catch (IllegalAccessException e) {
						}

						if (value != null) {
							this.extFields.put(name, value.toString());
						}
					}
				}
			}
		}
	}

	/**
	 * 序列化报文头
	 * 
	 * @return
	 */
	public ByteBuffer encodeHeader() {
		return encodeHeader(this.body != null ? this.body.length : 0);
	}

	/**
	 * 序列化报文头
	 * 
	 * @param bodyLength 报文体的长度
	 * @return
	 */
	public ByteBuffer encodeHeader(final int bodyLength) {
		// 1> header length size
		int length = 4;//4个字节来存储报文头的长度

		// 2> header data length
		byte[] headerData;
		headerData = this.headerEncode();

		length += headerData.length;

		// 3> body data length
		length += bodyLength;

		ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

		// length
		result.putInt(length);

		// header length
		result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

		// header data
		result.put(headerData);

		result.flip();//为读做准备

		return result;
	}

	public void markOnewayRPC() {
		int bits = 1 << RPC_ONEWAY;
		this.flag |= bits;
	}

	@JSONField(serialize = false)
	public boolean isOnewayRPC() {
		int bits = 1 << RPC_ONEWAY;
		return (this.flag & bits) == bits;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	@JSONField(serialize = false)
	public RemotingCommandType getType() {
		if (this.isResponseType()) {
			return RemotingCommandType.RESPONSE_COMMAND;
		}

		return RemotingCommandType.REQUEST_COMMAND;
	}

	@JSONField(serialize = false)
	public boolean isResponseType() {
		int bits = 1 << RPC_TYPE;
		return (this.flag & bits) == bits;
	}

	public LanguageCode getLanguage() {
		return language;
	}

	public void setLanguage(LanguageCode language) {
		this.language = language;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getOpaque() {
		return opaque;
	}

	public void setOpaque(int opaque) {
		this.opaque = opaque;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

	public String getRemark() {
		return remark;
	}

	public void setRemark(String remark) {
		this.remark = remark;
	}

	public byte[] getBody() {
		return body;
	}

	public void setBody(byte[] body) {
		this.body = body;
	}

	public HashMap<String, String> getExtFields() {
		return extFields;
	}

	public void setExtFields(HashMap<String, String> extFields) {
		this.extFields = extFields;
	}

	public void addExtField(String key, String value) {
		if (null == extFields) {
			extFields = new HashMap<String, String>();
		}
		extFields.put(key, value);
	}

	@Override
	public String toString() {
		return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque="
				+ opaque + ", flag(B)=" + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields="
				+ extFields + ", serializeTypeCurrentRPC=" + serializeTypeCurrentRPC + "]";
	}

	public SerializeType getSerializeTypeCurrentRPC() {
		return serializeTypeCurrentRPC;
	}

	public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
		this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
	}
}