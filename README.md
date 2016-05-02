# READ ME

## CookieTsai's Getting Started with Maven

[Getting Started with Maven](http://tsai-cookie.blogspot.tw/2016/03/getting-started-with-maven.html)

## Git Clone

```
$ git clone https://github.com/CookieTsai/MapReduce.git
```

## vim ~/.bash_profile

```
export JAVA_HOME=/c/Program\ Files/Java/jdk1.7.0_79

export PATH=$JAVA_HOME/bin:/c/Users/H205/Desktop/Lab/exe/apache-maven-3.3.9/bin:$PATH
```

## Make a Jar File

```
$ mvn clean package
```

## Run a MapReduce

```
$ hadoop jar <Main Class> <args>
```

## ETL 

```
$ tar -zxOf input/web.log.tar.gz | perl -ne 'print "$1\n" if /;act=order.+plist=([^;]+)/' | perl -ne 'print "$1,$2,$3\n" while /([0-9]+),([0-9]+),([0-9]+),?/g'
```

## Top 20

```
$ cat output/part-r-00000 | sort -k2 -rn | head -n 20| awk '{printf("%02d,%s\n",NR,$1)}'
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
