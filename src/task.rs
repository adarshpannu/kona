// task

use std::collections::HashMap;
use std::sync::mpsc;
use std::thread;
use std::thread::JoinHandle;

use crate::includes::*;
use crate::pop::*;

/***************************************************************************************************/
#[derive(Serialize, Deserialize)]
pub struct Task {
    pub partition_id: PartitionId,

    #[serde(skip)]
    pub contexts: HashMap<POPKey, NodeRuntime>,

    #[serde(skip)]
    pub task_row: Row,
}

// Tasks write to flow-id / top-id / dest-part-id / source-part-id
impl Task {
    pub fn new(partition_id: PartitionId) -> Task {
        Task {
            partition_id,
            contexts: HashMap::new(),
            task_row: Row::from(vec![]),
        }
    }

    pub fn run(&mut self, flow: &Flow, stage: &Stage) -> Result<(), String> {
        /*
        debug!(
            "Running task: stage = {:?}, partition = {}/{}",
            stage.root_pop_key, self.partition_id, stage.npartitions_producer
        );
        */
        self.task_row = Row::from(vec![Datum::NULL; 32]); // FIXME
        loop {
            let retval = stage.root_pop_key.unwrap().next(flow, stage, self, true)?;
            if !retval {
                break;
            }
        }
        Ok(())
    }
}

pub enum ThreadPoolMessage {
    RunTask(Vec<u8>),
    EndTask,
    TaskEnded,
}

/***************************************************************************************************/
pub struct ThreadPool {
    pub threads: Option<Vec<JoinHandle<()>>>,
    pub s2t_channels_sx: Vec<mpsc::Sender<ThreadPoolMessage>>, // scheduler -> threads (T channels i.e. one per thread)
    pub t2s_channel_rx: mpsc::Receiver<ThreadPoolMessage>,     // threads -> scheduler (1 channel, shared by all threads)
}

impl ThreadPool {
    pub fn join(&mut self) {
        let threads = self.threads.take();
        for thrd in threads.unwrap() {
            thrd.join().unwrap()
        }
    }

    pub fn size(&self) -> usize {
        if let Some(threads) = &self.threads {
            threads.len()
        } else {
            0
        }
    }

    pub fn close_all(&mut self) {
        for tx in self.s2t_channels_sx.iter() {
            tx.send(ThreadPoolMessage::EndTask).unwrap();
        }
    }

    pub fn new(nthreads: usize) -> ThreadPool {
        let mut threads = vec![];
        let mut s2t_channels_sx = vec![];

        let (t2s_channel_tx, t2s_channel_rx) = mpsc::channel::<ThreadPoolMessage>();

        for i in 0..nthreads {
            let t2s_channel_tx_clone = t2s_channel_tx.clone();

            let (s2t_channel_tx, s2t_channel_rx) = mpsc::channel::<ThreadPoolMessage>();

            let thrd = thread::Builder::new().name(format!("thread-{}", i)).spawn(move || {
                for msg in s2t_channel_rx {
                    match msg {
                        ThreadPoolMessage::EndTask => {
                            debug!("End of thread");
                            break;
                        }
                        ThreadPoolMessage::RunTask(encoded) => {
                            let (flow, stage, mut task): (Flow, Stage, Task) = bincode::deserialize(&encoded[..]).unwrap();

                            /*
                            debug!(
                                "Received task, len = {}, stage {}, partition {} ",
                                encoded.len(),
                                stage.head_node_id,
                                task.partition_id
                            );
                            */
                            task.run(&flow, &stage).unwrap();

                            // The following send may not succeed if the scheduler is gone
                            t2s_channel_tx_clone.send(ThreadPoolMessage::TaskEnded).unwrap_or_default()
                        }
                        ThreadPoolMessage::TaskEnded => {
                            panic!("Invalid message")
                        }
                    }
                }
            });
            threads.push(thrd.unwrap());
            s2t_channels_sx.push(s2t_channel_tx);

            //tx_channel.send(WorkerMessage::ShutdownWorker).unwrap();
        }
        ThreadPool {
            threads: Some(threads),
            s2t_channels_sx,
            t2s_channel_rx,
        }
    }
}
