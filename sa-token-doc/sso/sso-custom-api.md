# SSO整合-自定义 API 路由 

---

### 方式一：修改全局变量

在之前的章节中，我们演示了如何搭建一个SSO认证中心：
``` java
/**
 * Sa-Token-SSO Server端 Controller 
 */
@RestController
public class SsoServerController {

	// SSO-Server端：处理所有SSO相关请求 
	@RequestMapping("/sso/*")
	public Object ssoRequest() {
		return SaSsoProcessor.instance.serverDister();
	}
	
	// ... 其它代码
	
}
```

这种写法集成简单但却不够灵活。例如认证中心地址只能是：`http://{host}:{port}/sso/auth`，如果我们想要自定义其API地址，应该怎么做呢？

打开SSO模块相关源码，有关 API 的设计都定义在：
[ApiName.java](https://gitee.com/dromara/sa-token/blob/master/sa-token-plugin/sa-token-sso/src/main/java/cn/dev33/satoken/sso/name/ApiName.java)
中，我们可以对其进行二次修改。

例如，我们可以在 Main 方法启动类或者 SSO 配置方法中修改变量值：
``` java
// 配置SSO相关参数 
@Autowired
private void configSso(SaSsoConfig sso) {
	// 自定义API地址
	SaSsoUtil.ssoTemplate.apiName.ssoAuth = "/sso/auth2";
	// ... 
	
	// SSO 相关配置
	sso.setXxx ... ;
}
```

启动项目，统一认证地址就被我们修改成了：`http://{host}:{port}/sso/auth2`


### 方式二：拆分路由入口
根据上述路由入口：`@RequestMapping("/sso/*")`，我们给它起一个合适的名字 —— 聚合式路由。

与之对应的，我们可以将其修改为拆分式路由：

``` java
/**
 * Sa-Token-SSO Server端 Controller 
 */
@RestController
public class SsoServerController {

	// SSO-Server：统一认证地址 
	@RequestMapping("/sso/auth")
	public Object ssoAuth() {
		return SaSsoProcessor.instance.ssoAuth();
	}

	// SSO-Server：RestAPI 登录接口 
	@RequestMapping("/sso/doLogin")
	public Object ssoDoLogin() {
		return SaSsoProcessor.instance.ssoDoLogin();
	}

	// SSO-Server：校验ticket 获取账号id 
	@RequestMapping("/sso/checkTicket")
	public Object ssoCheckTicket() {
		return SaSsoProcessor.instance.ssoCheckTicket();
	}

	// SSO-Server：单点注销 
	@RequestMapping("/sso/signout")
	public Object ssoSignout() {
		return SaSsoProcessor.instance.ssoSignout();
	}
	
	// ... 其它方法 
	
}
```

拆分式路由 与 聚合式路由 在功能上完全等价，且提供了更为细致的路由管控。

