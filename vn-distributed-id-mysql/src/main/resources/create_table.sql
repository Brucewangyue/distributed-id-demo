--
create table `seq_id`(
  `id` bigint NOT NULL auto_increment,
  `value` varchar(1) DEFAULT NULL,
  primary key (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COMMENT='顺序自增id表'

--
create table `segment_id`(
   `id` bigint NOT NULL auto_increment,
   `max_id` bigint NOT NULL comment '当前最大id',
   `step` bigint NOT NULL comment '号段步长',
   `biz_code` varchar(16) NOT NULL comment '业务类型编码',
   `version` bigint NOT NULL comment '乐观锁版本',
   primary key (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='' auto_increment 1