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
package cn.dev33.satoken.solon.sso;

import cn.dev33.satoken.config.SaSsoConfig;
import cn.dev33.satoken.sso.SaSsoManager;
import cn.dev33.satoken.sso.SaSsoProcessor;
import cn.dev33.satoken.sso.SaSsoTemplate;
import cn.dev33.satoken.sso.SaSsoUtil;
import org.noear.solon.annotation.Bean;
import org.noear.solon.annotation.Condition;
import org.noear.solon.annotation.Configuration;
import org.noear.solon.annotation.Inject;
import org.noear.solon.core.AppContext;
import org.noear.solon.core.bean.InitializingBean;

/**
 * @author noear
 * @since 2.0
 */

@Condition(onClass = SaSsoManager.class)
@Configuration
public class SaSsoAutoConfigure implements InitializingBean {
    @Inject
    private AppContext appContext;

    @Override
    public void afterInjection() throws Throwable {
        appContext.subBeansOfType(SaSsoTemplate.class, bean->{
            SaSsoUtil.ssoTemplate = bean;
            SaSsoProcessor.instance.ssoTemplate = bean;
        });

        appContext.subBeansOfType(SaSsoConfig.class, bean->{
            SaSsoManager.setConfig(bean);
        });
    }

    /**
     * 获取 SSO 配置Bean
     * */
    @Bean
    public SaSsoConfig getConfig(@Inject(value = "${sa-token.sso}",required = false) SaSsoConfig ssoConfig) {
        return ssoConfig;
    }
}