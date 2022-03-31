# sandflow
Try to extend async [Stream](https://doc.rust-lang.org/std/stream/index.html) to do more things, e.g. map-reduce, iteration, and it can be executed with massive parallel in a distributed cluster;

## Example
```rust
use futures::StreamExt;

fn main() {
    let source = futures::stream::iter(vec![1, 2, 3, 4, 5, 6].into_iter()).map(|i| Ok(i));
    // this will run 'source.map(..).then(..)' in parallel(with 2 threads);
    let result = sandflow::spawn(2, source, || {
        move |src| {
            src.map(|item| item + 1)
                .then(|item| async move { Ok(item * 2) })
        }
    })
    .collect::<Vec<_>>();
    let r = futures::executor::block_on(result);
    println!("{:?}", r);
}
```

## TODO
- iteration 
- distirbute execution
