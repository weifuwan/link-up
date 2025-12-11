package org.apache.cockpit.config;

import org.apache.cockpit.common.utils.GitPropUtil;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.Contact;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
public class SwaggerConfig {


    @Bean
    public Docket createRestApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage("org.apache.cockpit.controller"))
                .paths(PathSelectors.any())
                .build()
                .enable(true);
    }

    private ApiInfo apiInfo() {
        String version = GitPropUtil.getProps(GitPropUtil.VERSION_FIELD_NAME);
        String commitId = GitPropUtil.getProps(GitPropUtil.COMMIT_ID_FIELD_NAME);

        return new ApiInfoBuilder()
                .title("Cockpit 接口文档")
                .description("欢迎使用Cockpit数据中台")
                .contact(new Contact("weifuwan", "", "295227940@qq.com"))
                .version(String.format("%s-%s", version == null ? "" : version, commitId == null ? "" : commitId))
                .build();
    }

}

