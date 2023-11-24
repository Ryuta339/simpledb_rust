# SimpleDB

- text: [Database Design and Implementation: Second Edition](https://www.amazon.co.jp/gp/product/3030338355/)
- code on Java: [The SimpleDB Database System](http://www.cs.bc.edu/~sciore/simpledb/)
- code on Rust: [cutsea110](https://github.com/cutsea110/simpledb)


## Some changes from [cutsea110](https://github.com/cutsea110/simpledb)

- In the [commit 1bee1b8](https://github.com/cutsea110/simpledb/commit/1bee1b8524b31fc1e7dbf6eb0f71fc5246163b8a), [this line](https://github.com/cutsea110/simpledb/blob/1bee1b8524b31fc1e7dbf6eb0f71fc5246163b8a/src/tx/recovery/logrecord/commit_record.rs#L39) is wrong. The following line is correct, and it is fixed in [a later commit 0a27169](https://github.com/cutsea110/simpledb/commit/0a271690dcb9978f9763848eef8e4a4e09f55528).
```rust
    p.set_i32(0, TxType::COMMIT as i32)?;
```


## Record Bytes

### SetStringRecord

TxType: 32 bits
TxNumber: 32 bits
FileName: length of filename + 32 bits
No. of Block: 32 bits
Offset: 32 bits
Value: length of the string value


### SetI32Record

TxType: 32 bits
TxNumber: 32 bits
FileName: length of filename + 32 bits
No. of Block: 32 bits
Offset: 32 bits
value: 32 bits


### StartRecord

TxType: 32 bits
TxNumber: 32 bits


### CommitRecord

TxType: 32 bits
TxNumber: 32 bits


### RollbackRecord

TxType: 32 bits
TxNumber: 32 bits


### CheckpointRecord

TxType: 32 bits



## For the future Record Bytes

### SetI15Record

### SetBoolRecord

### SetDateRecord
