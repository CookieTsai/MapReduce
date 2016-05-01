# READ ME

```
$ echo "foo foo quux labs foo bar quux" | python mapper.py
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
$ echo "foo foo quux labs foo bar quux" | python mapper.py | sort -k1,1 | python reducer.py
```

```
bar     1
foo     3
labs    1
quux    2
```


