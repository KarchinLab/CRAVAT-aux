DROP TABLE IF EXISTS `header_to_col`;
CREATE TABLE `header_to_col` (
  `level` varchar(10) DEFAULT NULL,
  `columngroup` varchar(50) DEFAULT NULL,
  `header` varchar(200) DEFAULT NULL,
  `col` varchar(50) DEFAULT NULL,
  `definition` varchar(30) DEFAULT NULL,
  `hidden` tinyint(12) DEFAULT NULL,
  `columnorder` int(11) DEFAULT NULL,
  `retfilt` tinyint(1) DEFAULT NULL,
  `retfilttype` varchar(10) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

LOAD DATA LOCAL INFILE '/ext/temp/Interactive_result_header_to_column_table_data.txt'
  INTO TABLE header_to_col;