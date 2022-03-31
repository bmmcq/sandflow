use futures::StreamExt;

fn main() {
    let source = futures::stream::iter(vec![1, 2, 3, 4, 5, 6].into_iter()).map(|i| Ok(i));
    let result = sandflow::spawn(2, source, || {
        |src| {
            src.map(|item| item + 1)
                .then(|item| async move { Ok(item * 2) })
        }
    })
    .collect::<Vec<_>>();
    let r = futures::executor::block_on(result);
    println!("{:?}", r);
}
