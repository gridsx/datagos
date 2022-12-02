CREATE TABLE `instances`
(
    `id`          int unsigned NOT NULL AUTO_INCREMENT,
    `host`        varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
    `port`        int                                                           NOT NULL,
    `username`    varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
    `password`    varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
    `state`       int unsigned NOT NULL COMMENT '0 创建 1 同步中  2 暂停   3停止',
    `position`    varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci          DEFAULT NULL,
    `dump_config` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
    `created`     timestamp                                                     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated`     timestamp                                                     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb3;


CREATE TABLE `mysql_tasks`
(
    `id`              int unsigned NOT NULL AUTO_INCREMENT,
    `name`            varchar(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
    `description`     varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
    `instance_id`     int unsigned NOT NULL,
    `table_filter`    text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
    `data_filter`     text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
    `mapping_config`  text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
    `target_instance` text CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
    `state`           int unsigned NOT NULL DEFAULT '0',
    `error_continue`  int unsigned NOT NULL,
    `created`         timestamp                                                     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated`         timestamp                                                     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8mb3;


INSERT INTO `datagos`.`instances`(`id`, `host`, `port`, `username`, `password`, `state`, `position`, `dump_config`,
                                  `created`, `updated`)
VALUES (2, '127.0.0.1', 3306, 'tuser', '1234zxcv', 1, '{\"Name\":\"binlog.000005\",\"Pos\":363207360}', '',
        '2022-11-18 11:27:02', '2022-11-29 10:29:54');


INSERT INTO `datagos`.`mysql_tasks`(`id`, `name`, `description`, `instance_id`, `table_filter`, `data_filter`,
                                    `mapping_config`, `target_instance`, `state`, `error_continue`, `created`,
                                    `updated`)
VALUES (2, '内存表同步', '不同实例同步', 2, '', '', '[{\"srcTable\":\"mem_tb\",\"dstTable\":\"mem_tb\"}]',
        '{\"host\":\"127.0.0.1\",\"port\":3306,\"username\":\"tuser\",\"password\":\"1234zxcv\",\"database\":\"test\"}',
        0, 1, '2022-11-14 08:39:24', '2022-11-14 11:34:24');
