Cases to check:
1. set overrides existing value
2. options of set command: xx, nx etc
3. expire command, update received to subscriber in case of expired keys
4. Handling all different key commands and their updates

Questions:
1. In case of rpop, if it's the last key, a key is deleted as well. so how do we do lrange or something?
Same  for others, basically del happens asynchronous, so should we do get? and if do should we error out?
2. 

List events:
sortstore, lpush, rpush, rpop, lpop, lset, linsert, lrem, ltrim,  
Hash events: hset, hincrby, hincrbyfloat, hdel, 
Set events: sadd, srem, sp##op, sinterstore, sunionstore, sdiffstore

> Note: get all emembers: smembers



Scenarios to test:
1. "set": 
   - no option: generates one event with payload set
   - EX - generates two event with payload "set" and "expire"
   > Note: After the key expires, another event is generated with payload "expired"

2. "del"
   - generates one "del" event per key deleted
   - returns integer denoting number of keys deleted

3. "rename"
   - generates two events: "rename_from" with pattern as source key and "rename_to" with pattern as destination key

4. "move"
   - generates "move_to" and "move_from" events
   > Note: in redis we only subscribe to 0 db so we don't get "move_to" notification

5. "copy"
   - generates a "copy_to" event with channel as new key

single key events:
set, setx, incr, decr, rename, copy

list events:
setrange, incrby
6. "expire
   - generates "expire" event on expire, "expired" when actually key expires
   - generates "del" event if negative value is given

7. "setrange"
   - generates a setrange event
8. "incr"
   - generates a incrby event
9. "append"
   - generates an "append" event
10. "lpush", "lpushx"
   - generates lpush event:
   > Note: the key being lpushed, u can't do get, u have to lrange as it's a list
11. "rpush", "rpushx"
   - generates rpush event:
   > Note: the key being rpushed, u can't do get, u have to lrange as it's a list
11. "sort"
   - generates sortstore event when store option is used, again resulting key is list, so can't do get on it

Commands to be tested:
1. DEL, RENAME, MOVE, COPY, RESTORE, EXPIRE(PEXPIRE, EXPIREAT, PEXPIREAT), 
2. SET ( (SETEX, SETNX,GETSET) )
3. MSET
4. SETRANGE
5. INCR, DECR, INCRBY, DECRBY
6. INCRBYFLOAT
7. APPEND
8. PERSIST

Events:
Updating keys:
COPY, SET, MSET, SETRANGE, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, APPEND, PERSIST, RESTORE, 

Deleting keys:
DEL, 

Common:
RENAME, MOVE, EXPIRE, 

set key1 val1
mset key1 val1 key2 val2
copy key1 key2 REPLACE
append key1 "-append"
setrange key1 4 "-offset"
expire key1 5
wait for 5 seconds

set key1 val2
expire key1 10
persist key1
expire key1 -2

set key1 val1
set key2 val2
rename key2 key1
move key1 1

set key1 1
incr key1
decr key1
incrby key1 2
decrby key1 2
incrbyfloat 0.5
del key1

func(key, operation){
   bool1 := client.DO(exists, key)
   bool2 := false
   if bool1 {
      bool2 := client.Do(Type, key) == operandType[operation]
   }
   if !bool1 or !bool2{
      client.DO(set default value)
      wait(1s)
      append(awaiting_messages,set, default value)
   }
   args := getargsforoperation(operation)
   client.DO(operation, key, args..)
   wait(1s)
   append(awaiting messages, operation, args)
}

testSubscribe(){
   for _,op := range(list_operations){
      func(key,op)
   }
   verify_messages(awaiting_messages)
}



set key1 1
incr key1
decr key1
incrby 