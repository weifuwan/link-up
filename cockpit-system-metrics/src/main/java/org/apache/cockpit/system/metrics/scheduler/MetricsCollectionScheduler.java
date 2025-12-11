package org.apache.cockpit.system.metrics.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.apache.cockpit.common.bean.po.metrics.MetricsJvmPO;
import org.apache.cockpit.system.metrics.service.MetricsJvmService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Component
@Slf4j
public class MetricsCollectionScheduler {

    @Autowired
    private MetricsJvmService metricsJvmService;

//    @Autowired
//    private MetricsSystemService metricsSystemService;
//
//    @Autowired
//    private MetricsBusinessService metricsBusinessService;

    @Value("${monitoring.application.name:default-app}")
    private String applicationName;

    @Value("${monitoring.instance.id:${spring.application.name}:${server.port}}")
    private String instanceId;

    @Value("${monitoring.collection.enabled:true}")
    private boolean collectionEnabled;

    @Value("${monitoring.collection.interval:60000}")
    private long collectionInterval;

    /**
     * JVM指标采集任务 - 每30秒执行一次
     */
    @Scheduled(fixedRateString = "${monitoring.jvm.interval:60000}")
    public void collectJvmMetrics() {
        if (!collectionEnabled) {
            return;
        }

        try {
            MetricsJvmPO jvmMetrics = metricsJvmService.collectJvmMetrics(applicationName, instanceId);
            if (jvmMetrics != null) {
                boolean success = metricsJvmService.saveJvmMetrics(jvmMetrics);
                if (success) {
                    log.debug("JVM指标数据保存成功");
                } else {
                    log.warn("JVM指标数据保存失败");
                }
            }
        } catch (Exception e) {
            log.error("JVM指标采集任务执行失败", e);
        }
    }

    /**
     * 数据清理任务 - 每天凌晨2点执行
     */
    @Scheduled(cron = "${monitoring.clean.cron:0 0 2 * * ?}")
    public void cleanExpiredData() {
        if (!collectionEnabled) {
            return;
        }

        try {
            // 清理30天前的数据
            LocalDateTime expireTime = LocalDateTime.now().minusDays(30);
            int jvmCount = metricsJvmService.cleanExpiredData(expireTime);
            // 其他表的清理...

            log.info("数据清理完成, 清理JVM记录: {} 条", jvmCount);
        } catch (Exception e) {
            log.error("数据清理任务执行失败", e);
        }
    }

    /**
     * 手动触发采集
     */
    public boolean triggerCollection() {
        try {
            collectJvmMetrics();
            // 可以添加其他指标的采集
            return true;
        } catch (Exception e) {
            log.error("手动触发采集失败", e);
            return false;
        }
    }
}
