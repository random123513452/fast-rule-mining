Code to verify the fast rule mining algorithm on YAGO dataset.

# requirements:
sbt >= 1.1.5

Spark version: 2.2.0

Steps to run the experiments:

# 1. untar the input dataset

```console
$ cd data
$ tar -xvf YAGO.tar.gz
$ cd ..
```
# 2. create the binary package
```console
$ sbt package
```

# 3. run experiment through bash file.
Notes:
You might need to change permission of the file "run.sh", and update spark binaries path and memory/cpu cores settings too.

```console
$ ./run.sh
```

The output rules with supp and confidence scores would be in "output/output_rules_str-i", i = 1, 2, 3, 4, 5, 6.

The format of the rules are of following:

1: p(x, y) <- q(x, y)

2: p(x, y) <- q(y, x)

3: p(x, y) <- q(z, x), r(z, y)

4: p(x, y) <- q(x, z), r(z, y)

5: p(x, y) <- q(z, x), r(y, z)

6: p(x, y) <- q(x, z), r(y, z)
