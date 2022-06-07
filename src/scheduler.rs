// scheduler

use std::sync::mpsc;
use std::thread;
use std::thread::JoinHandle;

use crate::includes::*;
use crate::pop::*;

#[derive(Debug)]
pub enum SchedulerMessage {
    RunTask(Vec<u8>),
    EndTask,
    TaskEnded { stage_id: StageId, partition_id: usize },
}

/***************************************************************************************************/
pub struct Scheduler {
    pub threads: Option<Vec<JoinHandle<()>>>,
    pub s2t_channels_sx: Vec<mpsc::Sender<SchedulerMessage>>, // scheduler -> threads (T channels i.e. one per thread)
    pub t2s_channel_rx: mpsc::Receiver<SchedulerMessage>,     // threads -> scheduler (1 channel, shared by all threads)
}

impl Scheduler {
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
            tx.send(SchedulerMessage::EndTask).unwrap();
        }
    }

    pub fn new(nthreads: usize) -> Scheduler {
        let mut threads = vec![];
        let mut s2t_channels_sx = vec![];

        let (t2s_channel_tx, t2s_channel_rx) = mpsc::channel::<SchedulerMessage>();

        for i in 0..nthreads {
            let t2s_channel_tx_clone = t2s_channel_tx.clone();

            let (s2t_channel_tx, s2t_channel_rx) = mpsc::channel::<SchedulerMessage>();

            let thrd = thread::Builder::new().name(format!("thread-{}", i)).spawn(move || {
                for msg in s2t_channel_rx {
                    match msg {
                        SchedulerMessage::EndTask => {
                            debug!("End of thread");
                            break;
                        }
                        SchedulerMessage::RunTask(encoded) => {
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
                            t2s_channel_tx_clone
                                .send(SchedulerMessage::TaskEnded {
                                    stage_id: stage.stage_id,
                                    partition_id: task.partition_id,
                                })
                                .unwrap_or_default()
                        }
                        SchedulerMessage::TaskEnded { .. } => {
                            panic!("Invalid message")
                        }
                    }
                }
            });
            threads.push(thrd.unwrap());
            s2t_channels_sx.push(s2t_channel_tx);

            //tx_channel.send(WorkerMessage::ShutdownWorker).unwrap();
        }
        Scheduler {
            threads: Some(threads),
            s2t_channels_sx,
            t2s_channel_rx,
        }
    }

    pub fn runnable<'a>(stages: &'a Vec<Stage>, stage_status: &Vec<StageStatus>) -> Vec<&'a Stage> {
        let v = stages
            .iter()
            .zip(stage_status.iter())
            .filter_map(|(stage, ss)| {
                if stage.orig_child_count == ss.completed_child_count && ss.completed_npartitions == 0 {
                    Some(stage)
                } else {
                    None
                }
            })
            .collect();
        v
    }

    pub fn run_flow(&self, env: &Env, flow: &Flow, stage_graph: &StageGraph) {
        let mut stage_status = (0..stage_graph.stages.len()).map(|_| StageStatus::new()).collect::<Vec<_>>();

        let stages = Self::runnable(&stage_graph.stages, &stage_status);
        for stage in stages {
            debug!("Running stage: {}", stage.stage_id);
            stage.run(env, flow);
        }

        for msg in &self.t2s_channel_rx {
            let mut stage_ended = None;

            debug!("run_flow message recv: {:?}", msg);

            match msg {
                SchedulerMessage::TaskEnded { stage_id, .. } => {
                    let mut ss = &mut stage_status[stage_id];
                    let stage = &stage_graph.stages[stage_id];

                    ss.completed_npartitions = ss.completed_npartitions + 1;
                    if stage.npartitions == ss.completed_npartitions {
                        stage_ended = Some(stage_id)
                    }
                }
                _ => {
                    panic!("Invalid message")
                }
            }

            if let Some(stage_ended) = stage_ended {
                debug!("run_flow stage ended: {:?}", stage_ended);
                return;
            }
        }
    }
}
