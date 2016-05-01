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

## Python

```
$ echo "foo foo quux labs foo bar quux" | python script/mapper.py
```

```
foo     1
foo     1
quux    1
labs    1
foo     1
bar     1
quux    1
```

```
$ echo "foo foo quux labs foo bar quux" | python script/mapper.py | sort -k1,1 | python script/reducer.py
```

```
bar     1
foo     3
labs    1
quux    2
```
