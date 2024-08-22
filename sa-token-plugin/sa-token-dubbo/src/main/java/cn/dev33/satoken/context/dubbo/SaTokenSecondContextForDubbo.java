/*
 * Copyright 2020-2099 sa-token.cc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.dev33.satoken.context.dubbo;

import org.apache.dubbo.rpc.RpcContext;

import cn.dev33.satoken.context.dubbo.model.SaRequestForDubbo;
import cn.dev33.satoken.context.dubbo.model.SaResponseForDubbo;
import cn.dev33.satoken.context.dubbo.model.SaStorageForDubbo;
import cn.dev33.satoken.context.model.SaRequest;
import cn.dev33.satoken.context.model.SaResponse;
import cn.dev33.satoken.context.model.SaStorage;
import cn.dev33.satoken.context.second.SaTokenSecondContext;
import cn.dev33.satoken.exception.ApiDisabledException;

/**
 * Sa-Token 二级上下文 [ Dubbo版本 ]
 * 
 * @author click33
 * @since 1.34.0
 */
public class SaTokenSecondContextForDubbo implements SaTokenSecondContext {

	@Override
	public SaRequest getRequest() {
		return new SaRequestForDubbo(RpcContext.getContext());
	}

	@Override
	public SaResponse getResponse() {
		return new SaResponseForDubbo(RpcContext.getContext());
	}

	@Override
	public SaStorage getStorage() {
		return new SaStorageForDubbo(RpcContext.getContext());
	}

	@Override
	public boolean matchPath(String pattern, String path) {
		throw new ApiDisabledException();
	}

	@Override
	public boolean isValid() {
		return RpcContext.getContext() != null;
	}
	
}
