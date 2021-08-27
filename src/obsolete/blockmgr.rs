// Block manager
#![allow(warnings)]

struct Block {
    payload: [u8; 1024],
}

impl Block {
    fn new() -> Box<Block> {
        Box::new(Block { payload: [0; 1024] })
    }
}

struct BlockMgr {
    free_list: Vec<Box<Block>>,
}

impl BlockMgr {
    fn new(init_block_count: u64) -> BlockMgr {
        let mut blocks = vec![];
        for i in 0..init_block_count {
            let block = Block::new();
            blocks.push(block);
        }
        BlockMgr { free_list: blocks }
    }

    

    fn get() -> Block {
        unimplemented!()
    }

    fn put() {

    }
}

#[test]
fn test() {
    let blkmgr = BlockMgr::new(10);
}

