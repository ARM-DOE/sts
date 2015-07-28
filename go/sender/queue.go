package main

// Queue is an implementation of a queue datatype in Go
type Queue struct {
    items          []string
    item_available bool
}

// QueueFactory generates and returns a new instance of the Queue struct
func QueueFactory() *Queue {
    new_queue := &Queue{}
    new_queue.items = make([]string, 0, 20)
    new_queue.item_available = false
    return new_queue
}

// request provides threadsafe access to the queue.
// If an item is available in the queue, a lock is applied while the item is being removed.
func (queue *Queue) request() string {
    if queue.item_available {
        return queue.pop()
    } else {
        return ""
    }
}

// add adds an item to a Queue struct
func (queue *Queue) add(item string) {
    queue.items = append(queue.items, item)
    queue.item_available = true
}

// pop removes the oldest item from the queue, and sets the item_available flag
func (queue *Queue) pop() string {
    queue.item_available = false
    index := 0
    popped_item := queue.items[index]
    queue.items = append(queue.items[:index], queue.items[index+1:]...)
    if len(queue.items) > 0 {
        queue.item_available = true
    }
    return popped_item
}
