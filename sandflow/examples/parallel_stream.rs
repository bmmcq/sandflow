use futures::StreamExt;
use sandflow::worker_index;

fn main() {
    let source = futures::stream::iter(vec![1, 2, 3, 4, 5, 6].into_iter()).map(|i| Ok(i));
    let result = sandflow::spawn(source, || {
        move |src| {
            src.map(|item| item + 1)
                .then(|item| async move { Ok(item + 1) })
                .exchange(|item| *item)
                .inspect(|item| println!("worker[{}]: {};", worker_index().unwrap(), item))
                .then(|item| async move { Ok(item * 2) })
        }
    })
    .collect::<Vec<_>>();
    let r = futures::executor::block_on(result);
    println!("{:?}", r);
}
