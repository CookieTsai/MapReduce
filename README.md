# READ ME

## Make a Jar File

```
mvn clean package
```

## ETL 

```
tar -zxOf input/web.log.tar.gz | perl -ne 'print "$1\n" if /;act=order.+plist=([^;]+)/' | perl -ne 'print "$1,$2,$3\n" while /([0-9]+),([0-9]+),([0-9]+),?/g'
```

## Top 20

```
cat output/part-r-00000 | sort -k2 -rn | head -n 20| awk '{printf("%02d,%s\n",NR,$1)}'
```