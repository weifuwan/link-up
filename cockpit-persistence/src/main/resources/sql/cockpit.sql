create
database if not exists cockpit;

DROP TABLE IF EXISTS `t_cockpit_datasource`;
CREATE TABLE t_cockpit_datasource
(
    `id`                varchar(32) NOT NULL COMMENT '主键',
    `db_name`           varchar(64)   DEFAULT NULL COMMENT '数据源名称',
    `db_type`           varchar(64)   DEFAULT NULL COMMENT '数据源类型',
    `original_json`     text          DEFAULT NULL COMMENT '原始JSON',
    `connection_params` text          DEFAULT NULL COMMENT '数据库连接参数',
    `environment`       varchar(200)  DEFAULT NULL COMMENT '环境',
    `remark`            varchar(2048) DEFAULT NULL COMMENT '描述',
    `conn_status`       varchar(24)   DEFAULT NULL COMMENT '连接状态',
    `create_by`         varchar(32)   DEFAULT NULL COMMENT '创建人',
    `create_time`       datetime      DEFAULT NULL COMMENT '创建时间',
    `update_by`         varchar(32)   DEFAULT NULL COMMENT '最后更新人',
    `update_time`       datetime      DEFAULT NULL COMMENT '最后更新时间',
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT ='数据集成-数据源';

DROP TABLE IF EXISTS `t_cockpit_integration_task_definition`;
CREATE TABLE t_cockpit_integration_task_definition
(
    `id`                  VARCHAR(32) NOT NULL COMMENT '主键',
    `name`                VARCHAR(128)  DEFAULT NULL COMMENT '任务名称',
    `task_params`         text          DEFAULT NULL COMMENT '流水线完整配置',
    `source_type`         VARCHAR(128)  DEFAULT NULL COMMENT 'source端类型',
    `source_id`           VARCHAR(32) NOT NULL COMMENT '源id',
    `sink_type`           VARCHAR(128)  DEFAULT NULL COMMENT 'sink端类型',
    `sink_id`             VARCHAR(32) NOT NULL COMMENT '端id',
    `fail_retry_times`    int           DEFAULT NULL COMMENT '失败重试次数',
    `fail_retry_interval` int           DEFAULT NULL COMMENT '失败重试间隔',
    `submit`              bit(1)        DEFAULT NULL COMMENT '是否提交：1（true）-是，0（false）-否',
    `task_execute_type`   varchar(100)  DEFAULT NULL COMMENT '任务执行类型',
    `remark`              varchar(2048) DEFAULT NULL COMMENT '描述',
    `create_by`           VARCHAR(32)   DEFAULT NULL COMMENT '创建人',
    `create_time`         DATETIME      DEFAULT NULL COMMENT '创建时间',
    `update_by`           VARCHAR(32)   DEFAULT NULL COMMENT '最后更新人',
    `update_time`         DATETIME      DEFAULT NULL COMMENT '最后更新时间',
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COMMENT ='数据同步-任务定义表';

DROP TABLE IF EXISTS `t_cockpit_integration_task_execution`;
CREATE TABLE `t_cockpit_integration_task_execution`
(
    `id`                  VARCHAR(32) NOT NULL COMMENT '主键',
    `definition_id`       VARCHAR(64) NOT NULL COMMENT '任务定义ID',
    `task_name`           VARCHAR(64)  DEFAULT NULL COMMENT '任务名称',
    `task_version`        INT          DEFAULT NULL COMMENT '任务版本号',
    `task_params`         text        NOT NULL COMMENT '执行时的配置快照',
    `status`              VARCHAR(32)  DEFAULT NULL COMMENT '执行状态',
    `engine_task_id`      VARCHAR(128) DEFAULT NULL COMMENT '引擎taskId',
    `log_path`            text         DEFAULT NULL COMMENT '执行日志目录',
    `execution_mode`      VARCHAR(32) COMMENT '执行模式: MANUAL-手动, SCHEDULED-调度',
    `start_time`          DATETIME     DEFAULT NULL COMMENT '开始时间',
    `end_time`            DATETIME     DEFAULT NULL COMMENT '结束时间',
    `source_total_record` bigint       DEFAULT NULL COMMENT 'source端总数',
    `sink_total_record`   bigint       DEFAULT NULL COMMENT 'sink端总数',
    `source_total_bytes`  bigint       DEFAULT NULL COMMENT 'source端字节数',
    `sink_total_bytes`    bigint       DEFAULT NULL COMMENT 'sink端字节数',
    `task_execute_type`   varchar(100) DEFAULT NULL COMMENT '任务执行类型',
    `create_by`           VARCHAR(32)  DEFAULT NULL COMMENT '创建人',
    `create_time`         DATETIME     DEFAULT NULL COMMENT '创建时间',
    `update_by`           VARCHAR(32)  DEFAULT NULL COMMENT '最后更新人',
    `update_time`         DATETIME     DEFAULT NULL COMMENT '最后更新时间',
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COMMENT ='数据同步-任务执行记录表';

DROP TABLE IF EXISTS `t_cockpit_datasource_plugin_config`;
CREATE TABLE t_cockpit_datasource_plugin_config
(
    `id`            VARCHAR(32) NOT NULL COMMENT '主键',
    `plugin_type`   VARCHAR(50) NOT NULL COMMENT '插件类型: mysql, postgresql, oracle, etc',
    `config_schema` text        NOT NULL COMMENT '配置字段的JSON schema',
    `create_by`     VARCHAR(32) DEFAULT NULL COMMENT '创建人',
    `create_time`   DATETIME    DEFAULT NULL COMMENT '创建时间',
    `update_by`     VARCHAR(32) DEFAULT NULL COMMENT '最后更新人',
    `update_time`   DATETIME    DEFAULT NULL COMMENT '最后更新时间',
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COMMENT ='数据同步-数据源插件动态配置表';

INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110002', 'MYSQL', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":3306,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"数据库名不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"password","label":"密码","type":"PASSWORD","required":true,"placeholder":"请输入密码","rules":[{"required":true,"message":"密码不能为空"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"mysql-connector-java-8.0.29.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]},{"key":"version","label":"版本","type":"SELECT","required":false,"defaultValue":"auto","options":[{"label":"自动选择","value":"auto"},{"label":"8.0.x / 5.7.x (驱动版本8.2.0) 推荐","value":"80_57_820"},{"label":"5.5.x / 5.6.x (驱动版本5.1.49) 推荐","value":"55_56_5149"},{"label":"5.5.x / 5.6.x (驱动版本5.1.46)","value":"55_56_5146)"}]},{"key":"other","label":"版本","type":"CUSTOM_SELECT","required":false,"defaultValue":[{"key":"useSSL","value":"false"},{"key":"allowPublicKeyRetrieval","value":"true"}]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110003', 'ORACLE', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":1521,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"数据库名不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"password","label":"密码","type":"PASSWORD","required":true,"placeholder":"请输入密码","rules":[{"required":true,"message":"密码不能为空"}]},{"key":"connectType","label":"连接类型","type":"SELECT","required":true,"defaultValue":"0","options":[{"value":"0","label":"ORACLE_SERVICE_NAME"},{"value":"1","label":"ORACLE_SID"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"ojdbc8-19.3.0.0.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110004', 'POSTGRESQL', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":5432,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"请输入数据库名称"}]},{"key":"schema","label":"schema","type":"INPUT","required":true,"placeholder":"默认为public","rules":[{"required":true,"message":"schema不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"password","label":"密码","type":"PASSWORD","required":true,"placeholder":"请输入密码","rules":[{"required":true,"message":"密码不能为空"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"postgresql-42.4.3.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]},{"key":"other","label":"连接扩展参数","type":"CUSTOM_SELECT","required":false,"defaultValue":[]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110005', 'OPENGAUSS', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":5432,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"数据库名不能为空"}]},{"key":"schema","label":"schema","type":"INPUT","required":true,"placeholder":"请输入schema","rules":[{"required":true,"message":"schema不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"password","label":"密码","type":"PASSWORD","required":true,"placeholder":"请输入密码","rules":[{"required":true,"message":"密码不能为空"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"opengauss-jdbc-6.0.2.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]},{"key":"other","label":"连接扩展参数","type":"CUSTOM_SELECT","required":false,"defaultValue":[]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110006', 'SQLSERVER', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":1433,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"数据库名不能为空"}]},{"key":"schema","label":"schema名","type":"INPUT","required":true,"placeholder":"请输入schema","rules":[{"required":true,"message":"schema不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"password","label":"密码","type":"PASSWORD","required":true,"placeholder":"请输入密码","rules":[{"required":true,"message":"密码不能为空"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"mssql-jdbc-9.4.1.jre8.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]},{"key":"other","label":"版本","type":"CUSTOM_SELECT","required":false,"defaultValue":[{"key":"useSSL","value":"false"},{"key":"allowPublicKeyRetrieval","value":"true"}]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110007', 'CACHE', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":1972,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"数据库名不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"password","label":"密码","type":"PASSWORD","required":true,"placeholder":"请输入密码","rules":[{"required":true,"message":"密码不能为空"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"cachejdbc.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]},{"key":"other","label":"版本","type":"CUSTOM_SELECT","required":false,"defaultValue":[{"key":"useSSL","value":"false"},{"key":"allowPublicKeyRetrieval","value":"true"}]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110008', 'DORIS', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":9030,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"数据库名不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"password","label":"密码","type":"PASSWORD","required":false,"placeholder":"请输入密码","rules":[{"required":false,"message":"密码不能为空"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"mysql-connector-java-8.0.29.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]},{"key":"version","label":"版本","type":"SELECT","required":false,"defaultValue":"auto","options":[{"label":"自动选择","value":"auto"},{"label":"8.0.x / 5.7.x (驱动版本8.2.0) 推荐","value":"80_57_820"},{"label":"5.5.x / 5.6.x (驱动版本5.1.49) 推荐","value":"55_56_5149"},{"label":"5.5.x / 5.6.x (驱动版本5.1.46)","value":"55_56_5146)"}]},{"key":"other","label":"版本","type":"CUSTOM_SELECT","required":false,"defaultValue":[{"key":"useSSL","value":"false"},{"key":"allowPublicKeyRetrieval","value":"true"}]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110009', 'STARROCKS', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":9030,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"数据库名不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"password","label":"密码","type":"PASSWORD","required":false,"placeholder":"请输入密码","rules":[{"required":false,"message":"密码不能为空"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"mysql-connector-java-8.0.29.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]},{"key":"other","label":"版本","type":"CUSTOM_SELECT","required":false,"defaultValue":[{"key":"useSSL","value":"false"},{"key":"allowPublicKeyRetrieval","value":"true"}]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110010', 'ELASTICSEARCH', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":9200,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"mysql-connector-java-8.0.29.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110011', 'CLICKHOUSE', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":8123,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"数据库名不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"password","label":"密码","type":"PASSWORD","required":true,"placeholder":"请输入密码","rules":[{"required":true,"message":"密码不能为空"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"clickhouse-jdbc-0.4.6.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110012', 'MONGODB', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":27017,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"数据库名不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"password","label":"密码","type":"PASSWORD","required":true,"placeholder":"请输入密码","rules":[{"required":true,"message":"密码不能为空"}]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110013', 'HIVE3', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":10000,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"数据库名不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"hive-jdbc-3.1.3.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110014', 'DB2', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":50000,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"数据库名不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"password","label":"密码","type":"PASSWORD","required":true,"placeholder":"请输入密码","rules":[{"required":true,"message":"密码不能为空"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"jcc-11.5.9.0.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');
INSERT INTO cockpit.t_cockpit_datasource_plugin_config
(id, plugin_type, config_schema, create_by, create_time, update_by, update_time)
VALUES('4fcb735cb0bf11f0a3850242ac110015', 'DAMENG', '{"fields":[{"key":"host","label":"主机地址IP","type":"INPUT","required":true,"placeholder":"请输入主机地址IP","defaultValue":"localhost","rules":[{"required":true,"message":"主机地址IP不能为空"}]},{"key":"port","label":"端口号","type":"NUMBER","required":true,"defaultValue":5236,"rules":[{"required":true,"message":"端口号不能为空"}]},{"key":"database","label":"数据库名","type":"INPUT","required":true,"placeholder":"请输入数据库名称","rules":[{"required":true,"message":"数据库名不能为空"}]},{"key":"username","label":"用户名","type":"INPUT","required":true,"placeholder":"请输入用户名","rules":[{"required":true,"message":"用户名不能为空"}]},{"key":"password","label":"密码","type":"PASSWORD","required":true,"placeholder":"请输入密码","rules":[{"required":true,"message":"密码不能为空"}]},{"key":"driverLocation","label":"驱动Jar包","type":"INPUT","required":true,"defaultValue":"DmJdbcDriver8.jar","placeholder":"请输入驱动Jar包","rules":[{"required":true,"message":"的jar包不能为空"}]}]}', 'system', '2025-10-24 17:53:37', 'system', '2025-10-24 17:53:37');

-- 创建任务调度关联表
DROP TABLE IF EXISTS `t_cockpit_integration_task_schedule`;
CREATE TABLE t_cockpit_integration_task_schedule
(
    `id`                 VARCHAR(32) NOT NULL COMMENT '主键',
    `task_definition_id` VARCHAR(32) NOT NULL COMMENT '任务定义ID',
    `cron_expression`    VARCHAR(50) NOT NULL COMMENT 'Cron表达式',
    `schedule_status`    VARCHAR(20) DEFAULT 'STOPPED' COMMENT '调度状态: STOPPED, RUNNING, PAUSED',
    `last_schedule_time` DATETIME COMMENT '最后调度时间',
    `next_schedule_time` DATETIME COMMENT '下次调度时间',
    `schedule_config`    TEXT COMMENT '调度配置信息',
    `create_by`          VARCHAR(32) DEFAULT NULL COMMENT '创建人',
    `create_time`        DATETIME    DEFAULT NULL COMMENT '创建时间',
    `update_by`          VARCHAR(32) DEFAULT NULL COMMENT '最后更新人',
    `update_time`        DATETIME    DEFAULT NULL COMMENT '最后更新时间',
    KEY                  idx_task_definition_id (task_definition_id),
    KEY                  idx_schedule_status (schedule_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务调度关联表';

-- JVM监控指标表 - 存储Java虚拟机监控数据
DROP TABLE IF EXISTS `t_cockpit_metrics_jvm`;
CREATE TABLE `t_cockpit_metrics_jvm`
(
    `id`                         varchar(32)   NOT NULL COMMENT '主键',
    `application_name`           varchar(100)  NOT NULL COMMENT '应用名称',
    `instance_id`                varchar(200)  NOT NULL COMMENT '实例ID(IP:PORT)',
    `heap_memory_init`           bigint         DEFAULT NULL COMMENT '堆内存初始大小(字节)',
    `heap_memory_used`           bigint        NOT NULL COMMENT '堆内存已使用(字节)',
    `heap_memory_committed`      bigint        NOT NULL COMMENT '堆内存已提交(字节)',
    `heap_memory_max`            bigint         DEFAULT NULL COMMENT '堆内存最大值(字节)',
    `heap_memory_usage`          decimal(5, 2) NOT NULL COMMENT '堆内存使用率(%)',
    `total_memory_used`          bigint         DEFAULT NULL COMMENT '总内存使用量(字节)',
    `total_memory_committed`     bigint         DEFAULT NULL COMMENT '总内存已提交(字节)',
    `total_memory_max`           bigint         DEFAULT NULL COMMENT '总内存最大值(字节)',
    `nonheap_memory_init`        bigint         DEFAULT NULL COMMENT '非堆内存初始大小(字节)',
    `nonheap_memory_used`        bigint        NOT NULL COMMENT '非堆内存已使用(字节)',
    `nonheap_memory_committed`   bigint        NOT NULL COMMENT '非堆内存已提交(字节)',
    `nonheap_memory_max`         bigint         DEFAULT NULL COMMENT '非堆内存最大值(字节)',
    `nonheap_memory_usage`       decimal(5, 2) NOT NULL COMMENT '非堆内存使用率(%)',
    `eden_memory_used`           bigint         DEFAULT NULL COMMENT 'Eden区内存使用(字节)',
    `eden_memory_max`            bigint         DEFAULT NULL COMMENT 'Eden区内存最大值(字节)',
    `survivor_memory_used`       bigint         DEFAULT NULL COMMENT 'Survivor区内存使用(字节)',
    `survivor_memory_max`        bigint         DEFAULT NULL COMMENT 'Survivor区内存最大值(字节)',
    `oldgen_memory_used`         bigint         DEFAULT NULL COMMENT '老年代内存使用(字节)',
    `oldgen_memory_max`          bigint         DEFAULT NULL COMMENT '老年代内存最大值(字节)',
    `oldgen_memory_usage`        decimal(5, 2)  DEFAULT NULL COMMENT '老年代内存使用率(%)',
    `metaspace_memory_used`      bigint         DEFAULT NULL COMMENT '元空间内存使用(字节)',
    `metaspace_memory_max`       bigint         DEFAULT NULL COMMENT '元空间内存最大值(字节)',
    `metaspace_memory_usage`     decimal(5, 2)  DEFAULT NULL COMMENT '元空间内存使用率(%)',
    `gc_young_count`             bigint         DEFAULT NULL COMMENT 'Young GC总次数',
    `gc_young_time`              bigint         DEFAULT NULL COMMENT 'Young GC总时间(毫秒)',
    `gc_young_count_increment`   bigint         DEFAULT NULL COMMENT '本次Young GC增量次数',
    `gc_young_frequency`         decimal(10, 2) DEFAULT NULL COMMENT 'Young GC频率(次/分钟)',
    `gc_old_count`               bigint         DEFAULT NULL COMMENT 'Old GC总次数',
    `gc_old_time`                bigint         DEFAULT NULL COMMENT 'Old GC总时间(毫秒)',
    `gc_old_count_increment`     bigint         DEFAULT NULL COMMENT '本次Old GC增量次数',
    `gc_old_frequency`           decimal(10, 2) DEFAULT NULL COMMENT 'Old GC频率(次/分钟)',
    `gc_full_count`              bigint         DEFAULT NULL COMMENT 'Full GC总次数',
    `gc_full_time`               bigint         DEFAULT NULL COMMENT 'Full GC总时间(毫秒)',
    `gc_full_count_increment`    bigint         DEFAULT NULL COMMENT '本次Full GC增量次数',
    `gc_full_frequency`          decimal(10, 2) DEFAULT NULL COMMENT 'Full GC频率(次/分钟)',
    `gc_last_duration`           bigint         DEFAULT NULL COMMENT '上次GC持续时间(毫秒)',
    `gc_last_type`               varchar(50)    DEFAULT NULL COMMENT '上次GC类型(YOUNG, OLD, FULL)',
    `thread_count`               int           NOT NULL COMMENT '当前线程数',
    `thread_peak_count`          int            DEFAULT NULL COMMENT '峰值线程数',
    `thread_daemon_count`        int            DEFAULT NULL COMMENT '守护线程数',
    `thread_started_count`       bigint         DEFAULT NULL COMMENT '启动以来总线程数',
    `thread_deadlock_count`      int            DEFAULT NULL COMMENT '死锁线程数',
    `thread_runnable_count`      int            DEFAULT NULL COMMENT '可运行状态线程数',
    `thread_blocked_count`       int            DEFAULT NULL COMMENT '阻塞状态线程数',
    `thread_waiting_count`       int            DEFAULT NULL COMMENT '等待状态线程数',
    `thread_timed_waiting_count` int            DEFAULT NULL COMMENT '定时等待状态线程数',
    `thread_new_count`           int            DEFAULT NULL COMMENT '新建状态线程数',
    `thread_terminated_count`    int            DEFAULT NULL COMMENT '终止状态线程数',
    `classes_loaded`             int            DEFAULT NULL COMMENT '已加载类数量',
    `classes_unloaded`           bigint         DEFAULT NULL COMMENT '已卸载类数量',
    `classes_total_loaded`       bigint         DEFAULT NULL COMMENT '总加载类数量',
    `compilation_time`           bigint         DEFAULT NULL COMMENT '编译总时间(毫秒)',
    `process_cpu_load`           decimal(5, 2)  DEFAULT NULL COMMENT '进程CPU使用率(%)',
    `system_cpu_load`            decimal(5, 2)  DEFAULT NULL COMMENT '系统CPU使用率(%)',
    `uptime`                     bigint         DEFAULT NULL COMMENT 'JVM运行时间(毫秒)',
    `collect_time`               datetime      NOT NULL COMMENT '数据采集时间',
    `collect_interval`           int            DEFAULT NULL COMMENT '采集间隔(毫秒)',
    `jvm_version`                varchar(100)   DEFAULT NULL COMMENT 'JVM版本',
    `gc_algorithm`               varchar(100)   DEFAULT NULL COMMENT 'GC算法',
    `create_by`                  varchar(32)    DEFAULT NULL COMMENT '创建人',
    `create_time`                datetime       DEFAULT NULL COMMENT '创建时间',
    `update_by`                  varchar(32)    DEFAULT NULL COMMENT '最后更新人',
    `update_time`                datetime       DEFAULT NULL COMMENT '最后更新时间',
    UNIQUE KEY `uk_instance_time` (`instance_id`,`collect_time`),
    KEY                          `idx_app_name` (`application_name`),
    KEY                          `idx_instance` (`instance_id`),
    KEY                          `idx_collect_time` (`collect_time`),
    KEY                          `idx_app_time` (`application_name`,`collect_time`),
    KEY                          `idx_gc_full` (`gc_full_count_increment`),
    KEY                          `idx_heap_usage` (`heap_memory_usage`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='JVM监控指标表';