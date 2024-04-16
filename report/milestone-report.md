# In Memory Parallel Hash Join Project Milestone Report
<center>Authors: Zhidong Guo (zhidongg), Ye Yuan (yeyuan3)</center>

## 1. Next Milestone Schedule


## 2. Work Completed So Far

### 2.1. Implementation of Hash Join Infrastructures

We have implemented the infrastructures for the sequential hash join and two variants of in-memory parallel hash join in Rust. The infrastructures include the following components:

- **Input File Reader**: 
- 

### 2.2. Implementation of Sequential Hash Join

We have implemented a sequential hash join algorithm as a baseline. The algorithm is implemented in Rust and is able to join two relations based on a join key. The pseudo code of the algorithm is as follows:

```text
/**
 * Pesudo-code for the sequential hash join algorithm
 * @param Table R: The smaller table to join
 * @param Table S: The larger table to join
 * @param Attribute R_key: The join attribute in table R
 * @param Attribute S_key: The join attribute in table S
 * @return Result: The joined table containing all matching rows
 */
func SequentialHashJoin(Table R, Table S, Attribute R_key, Attribute S_key):
    // Step 1: Build phase
    HashTable = {}

    // Loop over each record in the smaller table
    for each row in R:
        hashKey = hash(row[R_key])

        if hashKey not in HashTable:
            HashTable[hashKey] = []

        HashTable[hashKey].append(row)

    // Step 2: Probe phase
    Result = []

    // Loop over each record in the larger table
    for each row in S:
        hashKey = hash(row[S_key])

        if hashKey in HashTable:
            for R_row in HashTable[hashKey]:
                if R_row[R_key] == row[S_key]:
                    // Combine matching rows
                    joinedRow = join(R_row, row)

                    Result.append(joinedRow)

    return Result
```

#### 2.2.1. Complexity Analysis

- **Time Complexity**: Assume the smaller table `R` has $|R|$ rows and the larger table `S` has $|S|$ rows. For the build phase, the time complexity is $O(|R|)$. In the probe phase, the worst case rows in a hash bucket will be $|R|$ when rows are highly skewed. Therefore, the worst case time complexity of the probe phase is $O(|S| \cdot |R|)$. As a result, the overall time complexity of the sequential hash join algorithm is $O(|R| + |S| \cdot |R|)$.
- **Space Complexity**: The space complexity of the sequential hash join algorithm is $O(|R|)$ for the hash table.

#### 2.2.2. Performance Evaluation

We have evaluated the performance of the sequential hash join algorithm using synthetic data with different sizes and data skewness. The evaluation results are as follows:

**_PLACEHOLDER_**

### 2.3. Implementation of Benchmarking Framework

We have implemented a benchmarking framework in Python to evaluate the performance of the hash join algorithms. The framework is able to generate synthetic data with different data skewness and sizes, run the hash join algorithms on the data, and collect performance metrics such as execution time and memory usage. We also tested the framework with the sequential hash join algorithm and it is able to produce reasonable results.

### 2.4. Rust Language and Frameworks Onboarding

We have spent some time onboarding Rust language and frameworks. We have familiarized ourselves with the Rust syntax, data structures, and concurrency model. We have also explored some Rust libraries and frameworks that might be useful for our project, such as `rayon` for parallelism and `boxcar` for lock-free data structures implementation.

## 3. List of Goals

Considering the progress we have made so far is on track with our initial plan, we will continue to work on the following goals for the final presentation:

1. 

