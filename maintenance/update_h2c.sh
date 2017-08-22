TS=`date +%y-%m-%d-%H-%M-%S`
mysqldump -u root -p1 cravat_results header_to_col > /ext/temp/h2c_$TS.sql
mysql -u root -p1 --local-infile cravat_results < /ext/temp/update_h2c.sql