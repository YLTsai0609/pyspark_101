# Spark Join
[Optimizing Apache Spark SQL Joins: Spark Summit East talk by Vida Ha](https://www.youtube.com/watch?v=fp53QhSfQcI)

![img](assets/sparkjoin_1.png)

# Shuffle Hash Join

Most basic type of join

![img](assets/sparkjoin_2.png)

the key and the hash is a label to make data to an excutor and finally merge data.


![img](assets/sparkjoin_3.png)

![img](assets/sparkjoin_4.png)

Work best when : 

* Distribute evenly with the key you are joining on.
* Have an adequate number of keys for parallelism.
  * if you have 200M rows of data. you won't use 200M keys(a lot of shuffle steps)

## Shuffle Hash Join Performance Issue

![img](assets/sparkjoin_6.png)

![img](assets/sparkjoin_5.png)

since we use `state.name` as a join key.

big table : people in U.S.

small table : U.S. state

CA : a lot of people

RI : few people

![img](assets/sparkjoin_7.png)

You only have 50 keys.

If you increase your workers above 50. you won't get better performance

![img](assets/sparkjoin_8.png)

## Another case

![img](assets/sparkjoin_9.png)

![img](assets/sparkjoin_10.png)

![img](assets/sparkjoin_11.png)

## Detecting Shuffle Problems

![img](assets/sparkjoin_12.png)

check the output size evenly or not by `ls your_output_file`.

# Broadcast Hash Join

![img](assets/sparkjoin_13.png)

small table vs big table - use broadcast hash join 

![img](assets/sparkjoin_14.png)

# Cartesian Join

![img](assets/sparkjoin_15.png)

# One to Many Join

![img](assets/sparkjoin_16.png)

# Theta Join

Not the same key, but `keyA < KeyB + 10` for example

![img](assets/sparkjoin_17.png)
