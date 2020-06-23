# key-thread-pool-java

## A key-aware threadPoolExecutorService implement
- each submitted task must bind one key
- all tasks with same key will put in same queue executed sequence
- can expand or shrink queue count and change queue capacity dynamically
- can change core-thread size dynamically

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
