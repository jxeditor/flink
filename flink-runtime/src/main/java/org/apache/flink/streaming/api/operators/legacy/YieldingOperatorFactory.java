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

package org.apache.flink.streaming.api.operators.legacy;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/**
 * This class is no longer needed. {@link MailboxExecutor} is accessible via {@link
 * StreamOperatorParameters#getMailboxExecutor()}.
 *
 * <p>An operator that needs access to the {@link MailboxExecutor} to yield to downstream operators
 * needs to be created through a factory implementing this interface.
 */
@Internal
@Deprecated
public interface YieldingOperatorFactory<OUT> extends StreamOperatorFactory<OUT> {
    void setMailboxExecutor(MailboxExecutor mailboxExecutor);
}
