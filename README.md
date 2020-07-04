# key-thread-pool-java
[![Build Status](https://travis-ci.com/bizyun/key-thread-pool-java.svg?branch=master)](https://travis-ci.com/bizyun/key-thread-pool-java)
[![Coverage Status](https://coveralls.io/repos/github/bizyun/key-thread-pool-java/badge.svg?branch=master)](https://coveralls.io/github/bizyun/key-thread-pool-java?branch=master)
[![Total alerts](https://img.shields.io/lgtm/alerts/g/bizyun/key-thread-pool-java.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/bizyun/key-thread-pool-java/alerts/)
[![Language grade: Java](https://img.shields.io/lgtm/grade/java/g/bizyun/key-thread-pool-java.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/bizyun/key-thread-pool-java/context:java)

## A key-aware threadPoolExecutorService implementation
- each submitted task must bind one key
- all tasks with same key will put in same queue executed sequence
- expand or shrink queue count and change queue capacity dynamically
- change core-thread size dynamically

## Usage

```java
AtomicInteger poolSize = new AtomicInteger(1); // change core-thread size dynamically
AtomicInteger queueCapacity = new AtomicInteger(10); // change queue capacity dynamicallly 
AtomicInteger queueCount = new AtomicInteger(3); // change queue count dynamically
ExecutorService keyExecutor = KeyThreadPoolExecutor.newKeyThreadPool(() -> poolSize.get(),
                () -> new LinkedBlockingQueue<>(queueCapacity.get()), () -> queueCount.get());

keyExecutor.execute(new KeyRunnable() {
            @Override
            public long getKey() { // the key of the runnable
                return 0;
            }

            @Override
            public void run() {
            }
        });

KeyRunnable keyRunnable = new KeyRunnable() {
            @Override
            public long getKey() {
                return 0;
            }

            @Override
            public void run() {

            }
        };
keyExecutor.execute(keyRunnable::run);

keyExecutor.submit(new KeyCallable<Void>() {
            @Override
            public long getKey() { // the key of the callable
                return 0;
            }

            @Override
            public Void call() throws Exception {
                return null;
            }
        })
// work with CompletableFuture      
CompletableFuture.runAsync(new KeyRunnable() {
            @Override
            public long getKey() {
                return 0;
            }

            @Override
            public void run() {

            }
        }, keyExecutor);

// work with ListeningExecutorService
ListeningExecutorService keyListeningExecutor = MoreExecutors.listeningDecorator(keyExecutor);
keyListeningExecutor.submit(new KeyRunnable() {
            @Override
            public long getKey() {
                return 0;
            }

            @Override
            public void run() {

            }
        });
        
poolSize.set(20); // thread pool core size is 20
queueCount.set(1000); // queue count is 1000
queueCapacity.set(20000); // each queue capacity is 20000
```
