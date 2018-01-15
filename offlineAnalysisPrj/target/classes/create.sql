CREATE DATABASE `spark_project`;
USE `spark_project`;

#
#task表
#	用来存储J2EE平台插入其中的任务的信息
#
DROP TABLE IF EXISTS `task`;
CREATE TABLE `task` (
  `task_id` INT(11) NOT NULL AUTO_INCREMENT,
  `task_name` VARCHAR(255) DEFAULT NULL,
  `create_time` VARCHAR(255) DEFAULT NULL,
  `start_time` VARCHAR(255) DEFAULT NULL,
  `finish_time` VARCHAR(255) DEFAULT NULL,
  `task_type` VARCHAR(255) DEFAULT NULL,
  `task_status` VARCHAR(255) DEFAULT NULL,
  `task_param` TEXT,
  PRIMARY KEY (`task_id`)
) ENGINE=INNODB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;

#
#session_detail，
#	用来存储随机抽取出来的session的明细数据、top10品类的session的明细数据
#
DROP TABLE IF EXISTS `session_detail`;
CREATE TABLE `session_detail` (
  `task_id` INT(11) NOT NULL,
  `user_id` INT(11) DEFAULT NULL,
  `session_id` VARCHAR(255) DEFAULT NULL,
  `page_id` INT(11) DEFAULT NULL,
  `action_time` VARCHAR(255) DEFAULT NULL,
  `search_keyword` VARCHAR(255) DEFAULT NULL,
  `click_category_id` INT(11) DEFAULT NULL,
  `click_product_id` INT(11) DEFAULT NULL,
  `order_category_ids` VARCHAR(255) DEFAULT NULL,
  `order_product_ids` VARCHAR(255) DEFAULT NULL,
  `pay_category_ids` VARCHAR(255) DEFAULT NULL,
  `pay_product_ids` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

#
#top10_category_session表，
#	存储top10每个品类的点击top10的session
#
DROP TABLE IF EXISTS `top10_category_session`;
CREATE TABLE `top10_category_session` (
  `task_id` INT(11) NOT NULL,
  `category_id` INT(11) DEFAULT NULL,
  `session_id` VARCHAR(255) DEFAULT NULL,
  `click_count` INT(11) DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

#
#top10_category表
#	存储按点击、下单和支付排序出来的top10品类数据
#
DROP TABLE IF EXISTS `top10_category`;
CREATE TABLE `top10_category` (
  `task_id` INT(11) NOT NULL,
  `category_id` INT(11) DEFAULT NULL,
  `click_count` INT(11) DEFAULT NULL,
  `order_count` INT(11) DEFAULT NULL,
  `pay_count` INT(11) DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

#
#session_random_extract表
#	存储我们的按时间比例随机抽取功能抽取出来的1000个session
#
DROP TABLE IF EXISTS `session_random_extract`;
CREATE TABLE `session_random_extract` (
  `task_id` INT(11) NOT NULL,
  `session_id` VARCHAR(255) DEFAULT NULL,
  `start_time` VARCHAR(50) DEFAULT NULL,
  `end_time` VARCHAR(50) DEFAULT NULL,
  `search_keywords` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

#
#session_aggr_stat表
#	存储第一个功能，session聚合统计的结果
#
DROP TABLE IF EXISTS `session_aggr_stat`;
CREATE TABLE `session_aggr_stat` (
  `task_id` INT(11) NOT NULL,
  `session_count` INT(11) DEFAULT NULL,
  `1s_3s` DOUBLE DEFAULT NULL,
  `4s_6s` DOUBLE DEFAULT NULL,
  `7s_9s` DOUBLE DEFAULT NULL,
  `10s_30s` DOUBLE DEFAULT NULL,
  `30s_60s` DOUBLE DEFAULT NULL,
  `1m_3m` DOUBLE DEFAULT NULL,
  `3m_10m` DOUBLE DEFAULT NULL,
  `10m_30m` DOUBLE DEFAULT NULL,
  `30m` DOUBLE DEFAULT NULL,
  `1_3` DOUBLE DEFAULT NULL,
  `4_6` DOUBLE DEFAULT NULL,
  `7_9` DOUBLE DEFAULT NULL,
  `10_30` DOUBLE DEFAULT NULL,
  `30_60` DOUBLE DEFAULT NULL,
  `60` DOUBLE DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8;