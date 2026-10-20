import java.util.concurrent.*;

public class AsyncWorkerService {

    // The queue to hold incoming data. LinkedBlockingQueue is thread-safe.
    private final BlockingQueue<String> queue;

    // The thread pool that manages our N worker threads
    private final ExecutorService workerPool;

    // A single background thread to poll the queue and dispatch work
    private final ExecutorService dispatcher;

    private volatile boolean isRunning = true;

    public AsyncWorkerService(int numberOfWorkers, int queueCapacity) {
        this.queue = new LinkedBlockingQueue<>(queueCapacity);
        this.workerPool = Executors.newFixedThreadPool(numberOfWorkers);
        this.dispatcher = Executors.newSingleThreadExecutor();

        // Start the dispatcher immediately to listen for incoming queue data
        startDispatcher();
    }

    /**
     * PRODUCER: Accepts data asynchronously and pushes to the queue.
     * Returns a CompletableFuture so the caller isn't blocked.
     */
    public CompletableFuture<Void> acceptDataAsync(String data) {
        return CompletableFuture.runAsync(() -> {
            try {
                // put() blocks if the queue is full, preventing OutOfMemory errors
                queue.put(data);
                System.out.println("Data pushed to queue: " + data);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new CompletionException("Interrupted while adding to queue", e);
            }
        });
    }

    /**
     * Returns the number of items currently waiting in the queue.
     */
    public int getWaitingTaskCount() {
        return queue.size();
    }

    /**
     * Returns how much more space is left in the queue before it is full.
     */
    public int getRemainingCapacity() {
        return queue.remainingCapacity();
    }

    /**
     * DISPATCHER: Pops data from the queue and assigns it to a worker thread.
     */
    private void startDispatcher() {
        dispatcher.submit(() -> {
            while (isRunning || !queue.isEmpty()) {
                try {
                    // take() blocks until data is available, preventing CPU burn
                    String data = queue.take();

                    // Assign the popped data to one of the N worker threads
                    workerPool.submit(() -> processData(data));

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("Dispatcher interrupted.");
                }
            }
        });
    }

    /**
     * CONSUMER: The actual work performed by the worker threads.
     */
    private void processData(String data) {
        try {
            System.out.println("Processing '" + data + "' on thread: " + Thread.currentThread().getName());
            // Simulate heavy processing work
            Thread.sleep(1000);
            System.out.println("Finished '" + data + "'");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // --- Testing the flow ---
    public static void main(String[] args) {
        // Initialize service with 3 worker threads and a max queue size of 100
        AsyncWorkerService service = new AsyncWorkerService(3, 100);

        // Simulate receiving 10 asynchronous requests
        int x = 1;
        do {
            System.out.println("X Value : "+x);
            int i=1;
            for (; i <= x*x*x; i++) {
                service.acceptDataAsync("Payload-" + i);
            }

            // Check how many items are waiting after each batch
            System.out.println("Currently waiting in queue: " + service.getWaitingTaskCount());
            System.out.println("Remaining space in queue: " + service.getRemainingCapacity());

            // Add a small delay so you can observe the queue draining
            try { Thread.sleep(500); } catch (Exception e) {}

            x += x;
        } while(x % 64 != 0);
    }
}