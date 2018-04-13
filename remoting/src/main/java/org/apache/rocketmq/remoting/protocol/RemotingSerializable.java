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

import com.alibaba.fastjson.JSON;
import java.nio.charset.Charset;

/**
 * 远程传输序列化器提供了序列化其他对象的静态方法和序列化自身的方法。
 * 便于统一序列化策略，凡是需要远程传输的对象都继承此类
 *
 */
public abstract class RemotingSerializable {
    private final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");

    /**
     * 序列化
     * 
     * @param obj
     * @return
     */
    public static byte[] encode(final Object obj) {
        final String json = toJson(obj, false);
        if (json != null) {
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    /**
     * 对象转json
     * 
     * @param obj 
     * @param prettyFormat 是否漂亮格式
     * @return
     */
    public static String toJson(final Object obj, boolean prettyFormat) {
        return JSON.toJSONString(obj, prettyFormat);
    }

    /**
     * 反序列化
     * 
     * @param data
     * @param classOfT
     * @return
     */
    public static <T> T decode(final byte[] data, Class<T> classOfT) {
        final String json = new String(data, CHARSET_UTF8);
        return fromJson(json, classOfT);
    }

    /**
     * json转对象
     * 
     * @param json
     * @param classOfT
     * @return
     */
    public static <T> T fromJson(String json, Class<T> classOfT) {
        return JSON.parseObject(json, classOfT);
    }

    /**
     * 序列化自身
     * 
     * @return
     */
    public byte[] encode() {
        final String json = this.toJson();
        if (json != null) {
            return json.getBytes(CHARSET_UTF8);
        }
        return null;
    }

    /**
     * 将自身转化为json
     * 
     * @return
     */
    public String toJson() {
        return toJson(false);
    }

    /**
     * 将自身转化为json
     * 
     * @param prettyFormat 是否漂亮格式
     * @return
     */
    public String toJson(final boolean prettyFormat) {
        return toJson(this, prettyFormat);
    }
}
