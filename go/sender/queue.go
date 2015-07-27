package main

type Queue struct {
	items          []string
	item_available bool
}

func QueueFactory() *Queue {
	new_queue := &Queue{}
	new_queue.items = make([]string, 0, 20)
	new_queue.item_available = false
	return new_queue
}

func (queue *Queue) request() string {
	if queue.item_available {
		return queue.pop()
	} else {
		return ""
	}
}

func (queue *Queue) add(item string) {
	queue.items = append(queue.items, item)
	queue.item_available = true
}

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
