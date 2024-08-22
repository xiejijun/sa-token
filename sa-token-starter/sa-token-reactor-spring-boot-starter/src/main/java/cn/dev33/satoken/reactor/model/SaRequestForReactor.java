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
package cn.dev33.satoken.reactor.model;


import cn.dev33.satoken.SaManager;
import cn.dev33.satoken.application.ApplicationInfo;
import cn.dev33.satoken.context.model.SaRequest;
import cn.dev33.satoken.reactor.context.SaReactorHolder;
import cn.dev33.satoken.reactor.context.SaReactorSyncHolder;
import cn.dev33.satoken.util.SaFoxUtil;
import org.springframework.http.HttpCookie;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilterChain;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 对 SaRequest 包装类的实现（Reactor 响应式编程版）
 *
 * @author click33
 * @since 1.19.0
 */
public class SaRequestForReactor implements SaRequest {

	/**
	 * 底层Request对象
	 */
	protected ServerHttpRequest request;
	
	/**
	 * 实例化
	 * @param request request对象 
	 */
	public SaRequestForReactor(ServerHttpRequest request) {
		this.request = request;
	}
	
	/**
	 * 获取底层源对象 
	 */
	@Override
	public Object getSource() {
		return request;
	}

	/**
	 * 在 [请求体] 里获取一个值 
	 */
	@Override
	public String getParam(String name) {
		return request.getQueryParams().getFirst(name);
	}

	/**
	 * 获取 [请求体] 里提交的所有参数名称
	 * @return 参数名称列表
	 */
	@Override
	public List<String> getParamNames(){
		Set<String> names = request.getQueryParams().keySet();
		return new ArrayList<>(names);
	}

	/**
	 * 获取 [请求体] 里提交的所有参数
	 * @return 参数列表
	 */
	@Override
	public Map<String, String> getParamMap(){
		return request.getQueryParams().toSingleValueMap();
	}

	/**
	 * 在 [请求头] 里获取一个值 
	 */
	@Override
	public String getHeader(String name) {
		return request.getHeaders().getFirst(name);
	}

	/**
	 * 在 [Cookie作用域] 里获取一个值 
	 */
	@Override
	public String getCookieValue(String name) {
		HttpCookie cookie = request.getCookies().getFirst(name);
		if(cookie == null) {
			return null;
		}
		return cookie.getValue();
	}

	/**
	 * 返回当前请求path (不包括上下文名称)  
	 */
	@Override
	public String getRequestPath() {
		return ApplicationInfo.cutPathPrefix(request.getPath().toString());
	}

	/**
	 * 返回当前请求的url，例：http://xxx.com/test
	 * @return see note
	 */
	public String getUrl() {
		String currDomain = SaManager.getConfig().getCurrDomain();
		if( ! SaFoxUtil.isEmpty(currDomain)) {
			return currDomain + this.getRequestPath();
		}
		return request.getURI().toString();
	}
	
	/**
	 * 返回当前请求的类型 
	 */
	@Override
	public String getMethod() {
		return request.getMethod().name();
	}

	/**
	 * 转发请求 
	 */
	@Override
	public Object forward(String path) {
		ServerWebExchange exchange = SaReactorSyncHolder.getContext();
		WebFilterChain chain = exchange.getAttribute(SaReactorHolder.CHAIN_KEY);
		
		ServerHttpRequest newRequest = request.mutate().path(path).build();
		ServerWebExchange newExchange = exchange.mutate().request(newRequest).build();
		
		return chain.filter(newExchange); 
	}
	
}
