# sandflow
Try to extend the async [Stream](https://doc.rust-lang.org/std/stream/index.html) to do more things, e.g. map-reduce, iteration ... . 

More than that, the async stream can be executed with massive parallel in a distributed cluster;

## Example
```rust

fn main() {
    let source = futures::stream::iter(vec![1, 2, 3, 4, 5, 6].into_iter()).map(|i| Ok(i));
    // items in source stream will be consumed by parallel tasks in a round-robin manner;
    let result = sandflow::spawn(2, source, || {
        move |src| {
            src.map(|item| item + 1)
                .then(|item| async move { Ok(item + 1) })
                // exchange data between tasks; (e.g. for load balance;)
                .exchange(|item| *item)
                .inspect(| item | {
                    println!("worker[{}]: {};", worker_id(), item)
                })
                .then(|item|  async move { Ok (item * 2)})
        }
    })
    .collect::<Vec<_>>();
    let r = futures::executor::block_on(result);
    println!("{:?}", r);
}


```

