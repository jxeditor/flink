/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.legacy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AbstractRichFunction;

/**
 * A {@link org.apache.flink.api.common.functions.RichFunction} version of {@link SinkFunction}.
 *
 * @deprecated This interface will be removed in future versions. Use the new {@link
 *     org.apache.flink.api.connector.sink2.Sink} interface instead.
 */
@Internal
public abstract class RichSinkFunction<IN> extends AbstractRichFunction
        implements SinkFunction<IN> {

    private static final long serialVersionUID = 1L;
}
