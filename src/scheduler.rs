// scheduler

use std::{sync::mpsc, thread, thread::JoinHandle};

use crate::{
    includes::*,
    stage::{Stage, StageContext},
    task::Task,
    Flow,
};

#[derive(Debug)]
pub enum SchedulerMessage {
    ScheduleTask(Vec<u8>),
    TaskCompleted { stage_id: StageId, partition_id: usize },
    StageCompleted { stage_id: StageId },
    EndThread,
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

    pub fn nthreads(&self) -> usize {
        self.threads.as_ref().map_or(0, |threads| threads.len())
    }

    pub fn end_all_threads(&mut self) {
        for tx in self.s2t_channels_sx.iter() {
            tx.send(SchedulerMessage::EndThread).unwrap();
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
                        SchedulerMessage::EndThread => {
                            debug!("End of thread");
                            break;
                        }
                        SchedulerMessage::ScheduleTask(encoded) => {
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
                                .send(SchedulerMessage::TaskCompleted {
                                    stage_id: stage.stage_id,
                                    partition_id: task.partition_id,
                                })
                                .unwrap_or_default()
                        }
                        SchedulerMessage::TaskCompleted { .. } => {
                            panic!("Invalid message")
                        }
                        SchedulerMessage::StageCompleted { .. } => {
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

    pub fn runnable<'a>(stages: &'a Vec<Stage>, stage_status: &Vec<StageContext>) -> Vec<&'a Stage> {
        let v = stages
            .iter()
            .zip(stage_status.iter())
            .filter_map(|(stage, ss)| {
                if stage.nchildren == ss.nchildren_completed && ss.npartitions_completed == 0 {
                    Some(stage)
                } else {
                    None
                }
            })
            .collect();
        v
    }

    pub fn init_flow_tmpdir(&self, flow_id: usize) -> Result<(), String> {
        let dirname: &str = &format!("{}/flow-{}/", TEMPDIR, flow_id);

        // Add some protection against inadvertant deletion
        if dirname.find("tmp").is_none() {
            let errstr = f!("init_flow(): Temporary directory {TEMPDIR} doesn't have substring 'tmp'.");
            Err(errstr)
        } else {
            std::fs::remove_dir_all(dirname).map_err(|e| stringify1(e, &dirname)).unwrap_or_default();
            std::fs::create_dir_all(dirname).map_err(|e| stringify1(e, &dirname))?;
            Ok(())
        }
    }

    pub fn set_stage_completed(flow: &Flow, stage_contexts: &mut Vec<StageContext>, stage_id: StageId) {
        let stage_graph = &flow.stage_graph;
        if stage_id > 0 {
            let parent_stage_id = stage_graph.stages[stage_id].parent_stage_id.unwrap();
            stage_contexts[parent_stage_id].nchildren_completed = stage_contexts[parent_stage_id].nchildren_completed + 1;
        }
    }

    pub fn schedule_stages(&self, env: &Env, flow: &Flow, stage_contexts: &Vec<StageContext>) -> usize {
        let stage_graph = &flow.stage_graph;

        let stages = Self::runnable(&stage_graph.stages, &stage_contexts);
        for stage in stages.iter() {
            stage.schedule(env, flow);
        }
        stages.len()
    }

    pub fn run_flow(&self, env: &Env, flow: &Flow) -> Result<(), String> {
        self.init_flow_tmpdir(env.id)?;

        let stage_graph = &flow.stage_graph;
        let mut stage_contexts = (0..stage_graph.stages.len()).map(|_| StageContext::new()).collect::<Vec<_>>();

        self.schedule_stages(env, flow, &stage_contexts);

        for msg in &self.t2s_channel_rx {
            debug!("run_flow message recv: {:?}", msg);

            match msg {
                SchedulerMessage::TaskCompleted { stage_id, .. } => {
                    let mut ss = &mut stage_contexts[stage_id];
                    let stage = &stage_graph.stages[stage_id];

                    // If this was the last task in a stage, schedule any dependent stages
                    ss.npartitions_completed = ss.npartitions_completed + 1;
                    if stage.npartitions == ss.npartitions_completed {
                        debug!("Stage {} completed", stage_id);
                        Self::set_stage_completed(flow, &mut stage_contexts, stage_id);

                        debug!("Stage contexts: {:?}", &stage_contexts);
                        if self.schedule_stages(env, flow, &stage_contexts) == 0 {
                            break;
                        }
                    }
                }
                _ => {
                    panic!("Unexpected message received by scheduler.")
                }
            }
        }
        Ok(())
    }
}
