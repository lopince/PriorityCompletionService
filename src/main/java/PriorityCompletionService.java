import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class PriorityCompletionService<V> implements CompletionService<V> {

    private final Executor executor;
    private final Map<PriorityCompletionServicePriority, BlockingQueue<Future<V>>> priorityBlockingQueueMap;
    private final PriorityCompletionServicePriority defaultPriority;

    public PriorityCompletionService(Executor executor) {
        if (executor == null) {
            throw new NullPointerException();
        }
        this.executor = executor;
        this.priorityBlockingQueueMap = new HashMap<>();
        for (PriorityCompletionServicePriority priority : PriorityCompletionServicePriority.values()) {
            this.priorityBlockingQueueMap.put(priority, new LinkedBlockingDeque<>());
        }

        this.defaultPriority = PriorityCompletionServicePriority.LOW;
    }

    private class QueueingFuture extends FutureTask<Void> {

        private PriorityCompletionServicePriority priority;

        QueueingFuture(RunnableFuture<V> task, PriorityCompletionServicePriority priority) {
            super(task, null);
            this.task = task;
            this.priority = priority;
        }

        protected void done() {
            priorityBlockingQueueMap.get(priority).add(task);
        }

        private final Future<V> task;
    }

    private RunnableFuture<V> newTaskFor(Callable<V> task) {
        return new FutureTask<>(task);
    }

    private RunnableFuture<V> newTaskFor(Runnable task, V result) {
        return new FutureTask<>(task, result);
    }

    public Future<V> submit(Callable<V> task) {
        return submit(task, defaultPriority);
    }

    public Future<V> submit(Callable<V> task, PriorityCompletionServicePriority priority) {
        if (task == null) throw new NullPointerException();
        RunnableFuture<V> f = newTaskFor(task);
        executor.execute(new PriorityCompletionService.QueueingFuture(f, priority));
        return f;
    }

    public Future<V> submit(Runnable task, V result) {
        return submit(task, result, defaultPriority);
    }

    public Future<V> submit(Runnable task, V result, PriorityCompletionServicePriority priority) {
        if (task == null) throw new NullPointerException();
        RunnableFuture<V> f = newTaskFor(task, result);
        executor.execute(new PriorityCompletionService.QueueingFuture(f, priority));
        return f;
    }

    public Future<V> take() throws InterruptedException {
        return take(defaultPriority);
    }

    public Future<V> poll() {
        return poll(defaultPriority);
    }

    public Future<V> poll(long timeout, TimeUnit unit) throws InterruptedException {
        return poll(timeout, unit, defaultPriority);
    }

    public Future<V> take(PriorityCompletionServicePriority priority) throws InterruptedException {
        return priorityBlockingQueueMap.get(priority).take();
    }

    public Future<V> poll(PriorityCompletionServicePriority priority) {
        return priorityBlockingQueueMap.get(priority).poll();
    }

    public Future<V> poll(long timeout, TimeUnit unit, PriorityCompletionServicePriority priority) throws InterruptedException {
        return priorityBlockingQueueMap.get(priority).poll(timeout, unit);
    }
}
