
# word count in Apache Spark


```python
import pyspark
sc = pyspark.SparkContext(appName="wordCount")
```


```python
text_RDD = sc.textFile("/home/naveen/textfile1.txt")
```


```python
text_RDD.collect()
```




    [u'in a galaxy long time ago far far away in a some time we are going very far and not returning to our',
     u'home for a long time and nothing to do in peice']




```python
text_RDD = sc.textFile("textfile1.txt")
```


```python
text_RDD.collect()
```




    [u'in a galaxy long time ago far far away in a some time we are going very far and not returning to our',
     u'home for a long time and nothing to do in peice']




```python
def lower(line):
    return line.lower()

```


```python
lower_text_RDD = text_RDD.map(lower)
```


```python
lower_text_RDD.collect()
```




    [u'in a galaxy long time ago far far away in a some time we are going very far and not returning to our',
     u'home for a long time and nothing to do in peice']




```python
def split_words(line):
    return line.split()
```


```python
words_RDD = text_RDD.flatMap(split_words)
```


```python
words_RDD.collect()
```




    [u'in',
     u'a',
     u'galaxy',
     u'long',
     u'time',
     u'ago',
     u'far',
     u'far',
     u'away',
     u'in',
     u'a',
     u'some',
     u'time',
     u'we',
     u'are',
     u'going',
     u'very',
     u'far',
     u'and',
     u'not',
     u'returning',
     u'to',
     u'our',
     u'home',
     u'for',
     u'a',
     u'long',
     u'time',
     u'and',
     u'nothing',
     u'to',
     u'do',
     u'in',
     u'peice']




```python
def starts_with_a(word):
    return word.lower().startswith("a")
```


```python
words_RDD.filter(starts_with_a).collect()
```




    [u'a', u'ago', u'away', u'a', u'are', u'and', u'a', u'and']




```python
def create_pair(word):
    return (word, 1)
```


```python
pairs_RDD = words_RDD.map(create_pair)
```


```python
pairs_RDD.collect()
```




    [(u'in', 1),
     (u'a', 1),
     (u'galaxy', 1),
     (u'long', 1),
     (u'time', 1),
     (u'ago', 1),
     (u'far', 1),
     (u'far', 1),
     (u'away', 1),
     (u'in', 1),
     (u'a', 1),
     (u'some', 1),
     (u'time', 1),
     (u'we', 1),
     (u'are', 1),
     (u'going', 1),
     (u'very', 1),
     (u'far', 1),
     (u'and', 1),
     (u'not', 1),
     (u'returning', 1),
     (u'to', 1),
     (u'our', 1),
     (u'home', 1),
     (u'for', 1),
     (u'a', 1),
     (u'long', 1),
     (u'time', 1),
     (u'and', 1),
     (u'nothing', 1),
     (u'to', 1),
     (u'do', 1),
     (u'in', 1),
     (u'peice', 1)]




```python
pairs_RDD.groupByKey().collect()
pairs_RDD.cache()
```




    PythonRDD[31] at RDD at PythonRDD.scala:48




```python
for k,v in pairs_RDD.groupByKey().collect():
    print "Key :", k , ", Value : " , list(v)
```

    Key : a , Value :  [1, 1, 1]
    Key : ago , Value :  [1]
    Key : we , Value :  [1]
    Key : and , Value :  [1, 1]
    Key : for , Value :  [1]
    Key : far , Value :  [1, 1, 1]
    Key : away , Value :  [1]
    Key : some , Value :  [1]
    Key : long , Value :  [1, 1]
    Key : very , Value :  [1]
    Key : nothing , Value :  [1]
    Key : not , Value :  [1]
    Key : galaxy , Value :  [1]
    Key : do , Value :  [1]
    Key : peice , Value :  [1]
    Key : returning , Value :  [1]
    Key : home , Value :  [1]
    Key : in , Value :  [1, 1, 1]
    Key : to , Value :  [1, 1]
    Key : going , Value :  [1]
    Key : are , Value :  [1]
    Key : time , Value :  [1, 1, 1]
    Key : our , Value :  [1]



```python
def count_words(a, b):
    return a + b;
```


```python
count_words_RDD = pairs_RDD.reduceByKey(count_words)
```


```python
count_words_RDD.collect()
```




    [(u'a', 3),
     (u'ago', 1),
     (u'we', 1),
     (u'and', 2),
     (u'for', 1),
     (u'far', 3),
     (u'away', 1),
     (u'some', 1),
     (u'long', 2),
     (u'very', 1),
     (u'nothing', 1),
     (u'not', 1),
     (u'galaxy', 1),
     (u'do', 1),
     (u'peice', 1),
     (u'returning', 1),
     (u'home', 1),
     (u'in', 3),
     (u'to', 2),
     (u'going', 1),
     (u'are', 1),
     (u'time', 3),
     (u'our', 1)]




```python
pairs_RDD.take(1)
```




    [(u'in', 1)]




```python
groupkey_RDD = pairs_RDD.groupByKey()
```


```python
groupkey_RDD.collect()
```




    [(u'a', <pyspark.resultiterable.ResultIterable at 0x7f81fc1088d0>),
     (u'ago', <pyspark.resultiterable.ResultIterable at 0x7f81fc3cdd50>),
     (u'we', <pyspark.resultiterable.ResultIterable at 0x7f81fc185550>),
     (u'and', <pyspark.resultiterable.ResultIterable at 0x7f81fc185d50>),
     (u'for', <pyspark.resultiterable.ResultIterable at 0x7f81fc117b10>),
     (u'far', <pyspark.resultiterable.ResultIterable at 0x7f81fc117210>),
     (u'away', <pyspark.resultiterable.ResultIterable at 0x7f81fc117950>),
     (u'some', <pyspark.resultiterable.ResultIterable at 0x7f81fc1178d0>),
     (u'long', <pyspark.resultiterable.ResultIterable at 0x7f81fc117850>),
     (u'very', <pyspark.resultiterable.ResultIterable at 0x7f81fc117bd0>),
     (u'nothing', <pyspark.resultiterable.ResultIterable at 0x7f81fc117c10>),
     (u'not', <pyspark.resultiterable.ResultIterable at 0x7f81fc117c50>),
     (u'galaxy', <pyspark.resultiterable.ResultIterable at 0x7f81fc117c90>),
     (u'do', <pyspark.resultiterable.ResultIterable at 0x7f81fc117cd0>),
     (u'peice', <pyspark.resultiterable.ResultIterable at 0x7f81fc117d10>),
     (u'returning', <pyspark.resultiterable.ResultIterable at 0x7f81fc117d50>),
     (u'home', <pyspark.resultiterable.ResultIterable at 0x7f81fc117d90>),
     (u'in', <pyspark.resultiterable.ResultIterable at 0x7f81fc117dd0>),
     (u'to', <pyspark.resultiterable.ResultIterable at 0x7f81fc117e10>),
     (u'going', <pyspark.resultiterable.ResultIterable at 0x7f81fc117e50>),
     (u'are', <pyspark.resultiterable.ResultIterable at 0x7f81fc117e90>),
     (u'time', <pyspark.resultiterable.ResultIterable at 0x7f81fc117ed0>),
     (u'our', <pyspark.resultiterable.ResultIterable at 0x7f81fc117f10>)]




```python
for k, v in groupkey_RDD.collect():
    print "Key : " , k, ", Value : ", list(v)
```


    ---------------------------------------------------------------------------

    NameError                                 Traceback (most recent call last)

    <ipython-input-8-fa73e1c0e03f> in <module>()
    ----> 1 for k, v in groupkey_RDD.collect():
          2     print "Key : " , k, ", Value : ", list(v)


    NameError: name 'groupkey_RDD' is not defined



```python
wordcount_RDD = groupkey_RDD.reduceByKey(lambda a, b : a + b)
```


```python
wordcount_RDD.collect()
```




    [(u'a', <pyspark.resultiterable.ResultIterable at 0x7f81fc0ffd50>),
     (u'ago', <pyspark.resultiterable.ResultIterable at 0x7f81fc19d990>),
     (u'we', <pyspark.resultiterable.ResultIterable at 0x7f81fc3cddd0>),
     (u'and', <pyspark.resultiterable.ResultIterable at 0x7f81fc108b90>),
     (u'for', <pyspark.resultiterable.ResultIterable at 0x7f81fc108890>),
     (u'far', <pyspark.resultiterable.ResultIterable at 0x7f81fc108f50>),
     (u'away', <pyspark.resultiterable.ResultIterable at 0x7f81fc108cd0>),
     (u'some', <pyspark.resultiterable.ResultIterable at 0x7f81fc402f10>),
     (u'long', <pyspark.resultiterable.ResultIterable at 0x7f81fc117b90>),
     (u'very', <pyspark.resultiterable.ResultIterable at 0x7f81fc1179d0>),
     (u'nothing', <pyspark.resultiterable.ResultIterable at 0x7f81fc117910>),
     (u'not', <pyspark.resultiterable.ResultIterable at 0x7f81fc1177d0>),
     (u'galaxy', <pyspark.resultiterable.ResultIterable at 0x7f81fc117fd0>),
     (u'do', <pyspark.resultiterable.ResultIterable at 0x7f81fc117810>),
     (u'peice', <pyspark.resultiterable.ResultIterable at 0x7f81fc117b50>),
     (u'returning', <pyspark.resultiterable.ResultIterable at 0x7f81fc117990>),
     (u'home', <pyspark.resultiterable.ResultIterable at 0x7f81fc117090>),
     (u'in', <pyspark.resultiterable.ResultIterable at 0x7f81fc117890>),
     (u'to', <pyspark.resultiterable.ResultIterable at 0x7f81fc117790>),
     (u'going', <pyspark.resultiterable.ResultIterable at 0x7f81fc117350>),
     (u'are', <pyspark.resultiterable.ResultIterable at 0x7f81fc117750>),
     (u'time', <pyspark.resultiterable.ResultIterable at 0x7f81fc117710>),
     (u'our', <pyspark.resultiterable.ResultIterable at 0x7f81fc117ad0>)]




```python
for k, v in wordcount_RDD.collect():
    print "Key :", k , " , Value : " , list(v)
```

    Key : a  , Value :  [1, 1, 1]
    Key : ago  , Value :  [1]
    Key : we  , Value :  [1]
    Key : and  , Value :  [1, 1]
    Key : for  , Value :  [1]
    Key : far  , Value :  [1, 1, 1]
    Key : away  , Value :  [1]
    Key : some  , Value :  [1]
    Key : long  , Value :  [1, 1]
    Key : very  , Value :  [1]
    Key : nothing  , Value :  [1]
    Key : not  , Value :  [1]
    Key : galaxy  , Value :  [1]
    Key : do  , Value :  [1]
    Key : peice  , Value :  [1]
    Key : returning  , Value :  [1]
    Key : home  , Value :  [1]
    Key : in  , Value :  [1, 1, 1]
    Key : to  , Value :  [1, 1]
    Key : going  , Value :  [1]
    Key : are  , Value :  [1]
    Key : time  , Value :  [1, 1, 1]
    Key : our  , Value :  [1]



```python
def getSum(count):
    return (count[0], sum(count[1]))
```


```python
count_RDD = wordcount_RDD.map(getSum)
```


```python
count_RDD.collect()
```




    [(u'a', 3),
     (u'ago', 1),
     (u'we', 1),
     (u'and', 2),
     (u'for', 1),
     (u'far', 3),
     (u'away', 1),
     (u'some', 1),
     (u'long', 2),
     (u'very', 1),
     (u'nothing', 1),
     (u'not', 1),
     (u'galaxy', 1),
     (u'do', 1),
     (u'peice', 1),
     (u'returning', 1),
     (u'home', 1),
     (u'in', 3),
     (u'to', 2),
     (u'going', 1),
     (u'are', 1),
     (u'time', 3),
     (u'our', 1)]




```python
accum = sc.accumulator(0)
```


```python
def test_accum(x):
    accum.add(x)
```


```python
sc.parallelize([1,2,3,4]).foreach(test_accum)
```


```python
accum.value
```




    10


