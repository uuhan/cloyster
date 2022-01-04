use serde::{Deserialize, Serialize};
use cloyster::pagecache::{self, pin, Config, Materializer};

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug, Serialize, Deserialize)]
pub struct TestState(String);

impl Materializer for TestState {
    // Used to merge chains of partial pages into a form
    // that is useful for the `PageCache` owner.
    fn merge(&mut self, other: &TestState) {
        self.0.push_str(&other.0);
    }
}

fn main() {
    env_logger::init();

    let config = pagecache::ConfigBuilder::new().path("./cloyster.db").build();
    let pc: pagecache::PageCache<TestState> = pagecache::PageCache::start(config).unwrap();
    {
        // We begin by initiating a new transaction, which
        // will prevent any witnessable memory from being
        // reclaimed before we drop this object.
        let guard = pin();

        // The first item in a page should be set using allocate,
        // which signals that this is the beginning of a new
        // page history.
        let (id, mut key) = pc.allocate(TestState("a".to_owned()), &guard).unwrap();

        // Subsequent atomic updates should be added with link.
        key = pc.link(id, key, TestState("b".to_owned()), &guard).unwrap().unwrap();
        key = pc.link(id, key, TestState("c".to_owned()), &guard).unwrap().unwrap();

        // println!("ID: {}", id);

        // When getting a page, the provided `Materializer` is
        // used to merge all pages together.

        // let id = 12;
        let (mut key, page, size_on_disk) = pc.get(id, &guard).unwrap().unwrap();

        println!("get id {}: {}", id, page.0);

        // assert_eq!(page.0, "abc".to_owned());

        // You can completely rewrite a page by using `replace`:
        // key = pc.replace(id, key, TestState("d".into()), &guard).unwrap().unwrap();

        // let (key, page, size_on_disk) = pc.get(id, &guard).unwrap().unwrap();

        // assert_eq!(page.0, "d".to_owned());
    }
}
