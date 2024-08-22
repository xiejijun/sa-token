package com.pj.h5;


import cn.dev33.satoken.sso.SaSsoConsts;
import cn.dev33.satoken.sso.SaSsoUtil;
import cn.dev33.satoken.stp.StpUtil;
import cn.dev33.satoken.util.SaFoxUtil;
import cn.dev33.satoken.util.SaResult;
import org.noear.solon.annotation.Controller;
import org.noear.solon.annotation.Mapping;
import org.noear.solon.core.handle.Context;
import org.noear.solon.core.handle.Render;

/**
 * 前后台分离架构下集成SSO所需的代码 （SSO-Server端）
 * <p>（注：如果不需要前后端分离架构下集成SSO，可删除此包下所有代码）</p>
 * @author click33
 *
 */
@Controller
public class H5Controller implements Render {

	/**
	 * 获取 redirectUrl
	 */
	@Mapping("/sso/getRedirectUrl")
	private Object getRedirectUrl(String redirect, String mode, String client) {
		// 未登录情况下，返回 code=401 
		if (StpUtil.isLogin() == false) {
			return SaResult.code(401);
		}
		// 已登录情况下，构建 redirectUrl 
		if (SaSsoConsts.MODE_SIMPLE.equals(mode)) {
			// 模式一 
			SaSsoUtil.checkRedirectUrl(SaFoxUtil.decoderUrl(redirect));
			return SaResult.data(redirect);
		} else {
			// 模式二或模式三 
			String redirectUrl = SaSsoUtil.buildRedirectUrl(StpUtil.getLoginId(), client, redirect);
			return SaResult.data(redirectUrl);
		}
	}

	/**
	 * 控制当前类的异常
	 */
	@Override
	public void render(Object data, Context ctx) throws Throwable {
		if (data instanceof Throwable) {
			Throwable e = (Throwable) data;
			e.printStackTrace();
			ctx.render(SaResult.error(e.getMessage()));
		} else {
			ctx.render(data);
		}
	}
}
