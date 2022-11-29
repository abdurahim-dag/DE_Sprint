docker cp ./*
hadoop fs -put ./* /user/ragim/
hadoop fs -ls /user/ragim
hadoop fs -getmerge /user/ragim/*.txt ~/merged.txt
hadoop fs -chmod 0764 /user/ragim/merged.txt
hadoop fs -du -h /user/ragim/merged.txt
hadoop fs -setrep 2 /user/ragim/merged.txt