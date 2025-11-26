use std::collections::VecDeque;

pub trait Replacer {
    fn pick_victim(&mut self) -> Option<usize>;
    fn pin(&mut self, frame_id: usize);
    fn unpin(&mut self, frame_id: usize);
}

// LRU 替换策略
#[derive(Clone)]
pub struct LRU {
    frames: VecDeque<usize>,    // 未被 pin 的 frame 队列
    pinned: std::collections::HashSet<usize>, // 已被 pin 的 frame
}

impl LRU {
    pub fn new(pool_size: usize) -> Self {
        let mut frames = VecDeque::new();
        for i in 0..pool_size {
            frames.push_back(i);
        }
        LRU {
            frames,
            pinned: std::collections::HashSet::new(),
        }
    }
}

impl Replacer for LRU {
    // 选择一个受害者帧（LRU 策略：选择队列前端最久未使用的）
    fn pick_victim(&mut self) -> Option<usize> {
        // 从队列前端获取最久未使用的帧
        self.frames.pop_front()
    }

    // 标记帧被使用（pin）
    fn pin(&mut self, frame_id: usize) {
        self.pinned.insert(frame_id);
    }

    // 取消标记帧（unpin），将其加入队列末尾
    fn unpin(&mut self, frame_id: usize) {
        if self.pinned.remove(&frame_id) {
            self.frames.push_back(frame_id);
        }
    }
}
