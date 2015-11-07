USE bcia_statis;

--
-- Table structure for table `profession_result_segmt`
--

DROP TABLE IF EXISTS `profession_result_segmt`;
CREATE TABLE `profession_result_segmt` (
  `id` int(8) NOT NULL auto_increment,
  `segmt` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `min_duar` varchar(5) NOT NULL,
  `max_duar` varchar(5) NOT NULL,
  `avg_duar` varchar(5) NOT NULL,
  `pass_rate` varchar(5) NOT NULL,
  `median_duar` varchar(5) NOT NULL,
  `process_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),UNIQUE KEY (segmt)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `profession_result_segmt_bracket`
--

DROP TABLE IF EXISTS `profession_result_segmt_bracket`;
CREATE TABLE `profession_result_segmt_bracket` (
  `id` int(8) NOT NULL auto_increment,
  `level_id` varchar(2) NOT NULL,
  `segmt` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `min_duar` varchar(5) NOT NULL,
  `max_duar` varchar(5) NOT NULL,
  `avg_duar` varchar(5) NOT NULL,
  `pass_rate` varchar(5) NOT NULL,
  `process_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),UNIQUE KEY (level_id,segmt)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;


--
-- Table structure for table `profession_result_segmt_barcode`
--

DROP TABLE IF EXISTS `profession_result_segmt_barcode`;
CREATE TABLE `profession_result_segmt_barcode` (
  `id` int(8) NOT NULL auto_increment,
  `segmt` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `gate_id` varchar(7) NOT NULL,
  `pass_num` varchar(5) NOT NULL,
  `process_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),UNIQUE KEY (segmt,gate_id)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `profession_result_segmt_sck`
--

DROP TABLE IF EXISTS `profession_result_segmt_sck`;
CREATE TABLE `profession_result_segmt_sck` (
  `id` int(8) NOT NULL auto_increment,
  `sck_field` varchar(8) NOT NULL,
  `segmt` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `min_duar` varchar(5) NOT NULL,
  `max_duar` varchar(5) NOT NULL,
  `avg_duar` varchar(5) NOT NULL,
  `pass_rate` varchar(5) NOT NULL,
  `load_rate` float NOT NULL,
  `median_duar` varchar(5) NOT NULL,
  `top5_avg_duar` varchar(5) NOT NULL,
  `process_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),UNIQUE KEY (sck_field,segmt)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `wifi_result_segmt`
--

DROP TABLE IF EXISTS `wifi_result_segmt`;
CREATE TABLE `wifi_result_segmt` (
  `id` int(8) NOT NULL auto_increment,
  `segmt` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `min_duar` varchar(5) NOT NULL,
  `max_duar` varchar(5) NOT NULL,
  `avg_duar` varchar(5) NOT NULL,
  `pass_rate` varchar(5) NOT NULL,
  `median_duar` varchar(5) NOT NULL,
  `process_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),UNIQUE KEY (segmt)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `wifi_result_segmt_bracket`
--

DROP TABLE IF EXISTS `wifi_result_segmt_bracket`;
CREATE TABLE `wifi_result_segmt_bracket` (
  `id` int(8) NOT NULL auto_increment,
  `level_id` varchar(2) NOT NULL,
  `segmt` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `min_duar` varchar(5) NOT NULL,
  `max_duar` varchar(5) NOT NULL,
  `avg_duar` varchar(5) NOT NULL,
  `pass_rate` varchar(5) NOT NULL,
  `process_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),UNIQUE KEY (level_id,segmt)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- Dump completed on 2015-11-04  9:32:21
