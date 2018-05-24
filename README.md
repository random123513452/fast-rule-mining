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

The outputed rules with supp and confidence scores would be in "output/output_rules_str-%d".
