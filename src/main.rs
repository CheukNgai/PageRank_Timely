extern crate timely;

use std::io::BufReader;
use std::io::BufRead;
use std::fs::File;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::{Probe, ConnectLoop,LoopVariable};
use timely::dataflow::channels::pact::Exchange;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {
        // get all edge and the node_number
        let (edge_list, node_number) = get_edge_list_from_path("./resource/test1.net".to_string());
        let max_iteration = 50;
        let damping_factor = 0.85;
        
        // default PR value
        let default_PR:f32 = 1.0 / node_number as f32;
        // create input and output handles.
        let mut edge_input = InputHandle::new();

        // probe to monitor the computation process
        let mut probe = ProbeHandle::new();

        // build a new dataflow.
        // input is (src, dst) pair.
        worker.dataflow(|scope| {
        	//edge_stream provide (src, dst) data
            let edge_stream = edge_input.to_stream(scope);

            // create a new feedback stream, apply the feedback to the rank data
            let (handle, feedback_stream) = scope.loop_variable(max_iteration, 1);

            // define new operator with binary input
            // output the pair(dst, delta), whose dalta is the changes applied to the rank 
            // partition the data according the source node
            let output = edge_stream.binary_frontier(
                &feedback_stream,
                Exchange::new(|x: &(String, String)| hash(&x.0) as u64),
                Exchange::new(|x: &(String, f32)| hash(&x.0) as u64),
                "PageRank",
                |_capability, _info| {
                	// where we stash the data according with Time as key
                    let mut edge_stash = HashMap::new();
                    let mut rank_stash = HashMap::new();

                    // Map the node:String to a list of dst
                    let mut edges: HashMap<String, HashSet<String> > = HashMap::new();

                    // Map the node:String to its rank
                    let mut ranks: HashMap<String, f32> = HashMap::new();

                    //the logic of this operator
                    move |input1, input2, output| {

                    	// hold on to edge according to time.
                        input1.for_each(|time, data| {
                            edge_stash.entry(time.retain()).or_insert(Vec::new()).extend(data.drain(..));
                        });

                        // hold on to feedback changes according to time.
                        input2.for_each(|time, data| {
                            rank_stash.entry(time.retain()).or_insert(Vec::new()).extend(data.drain(..));
                        });

                        // the frontier contains the frontier comes from the inputs
                        let frontiers = &[input1.frontier(), input2.frontier()];

                        // 1.receive all the edge_changes
                        // 2.renew the edge list according the (src, dst)
                        // 3.emit the (dst, diff)
                        for (time, edge_changes) in edge_stash.iter_mut() {
                            if frontiers.iter().all(|f| !f.less_equal(time)) {
                            	//initial a session for further output(dst, delta)

                                println!("in the block 1 \n\n");
                            	let mut session = output.session(time);
                            	for (src, dst) in edge_changes.drain(..) {
                            		// add this edge to the edges if not exist
                            		// allocate the memory for the rank[src] rank[dst]
                            		edges.entry(src.clone()).or_insert(HashSet::new()).insert(dst.clone());
                            		edges.entry(dst.clone()).or_insert(HashSet::new());
                            		ranks.entry(src.clone()).or_insert(default_PR);
                            		ranks.entry(dst.clone()).or_insert(default_PR);
                            	}
                                println!("{:?}", edges);

                            	for (src, dsts)in edges.iter_mut() {
                            		for dst in dsts.iter() {
                            			let mut delta = ranks[src].clone();
                            			session.give((dst.clone(), delta));
                            			println!("{} send {} the rank {} at {:?}", src , dst, delta, time);
                            		}
                            	}
                            }
                        }
                        edge_stash.retain(|_key, val| !val.is_empty());
                        // 1. receive all the rank change
                        // 2. store the diff of the rank
                        // 3. calculate the cumulative rank
                        // 4. renew the the rank
                        // 5. emit the pair (dst, diff) as the input of next iteration 

                        let mut new_ranks: HashMap<String, f32> = HashMap::new();
                        for (time, rank_changes) in rank_stash.iter_mut() {
                            if frontiers.iter().all(|f| !f.less_equal(time)) {
                                let mut session = output.session(time);

                                
                                for (dst, diff) in rank_changes.iter_mut() {
                                    // allocate the memory for the rank[dst]
                                    ranks.entry(dst.clone()).or_insert(default_PR);
                                    new_ranks.entry(dst.clone()).or_insert(0.0);
                                }

                                // calculate the cumulative sum

                                for (dst, diff) in rank_changes.drain(..) {
                                    *new_ranks.entry(dst.clone()).or_insert(0.0) += diff.clone();
                                }

                                for (dst, cum_sum) in new_ranks.iter_mut() {
                                    
                                    // using cumulative sum to calculate the new PR
                                    *cum_sum = damping_factor * (*cum_sum) + (1.0 - damping_factor) * default_PR;
                                    *ranks.entry(dst.clone()).or_insert(default_PR) = *cum_sum;
                                }

                                // emit the rank changes
                                println!("Start to emit the rank:");
                                for (src, dsts)in edges.iter_mut() {

                                    let mut dst_num = dsts.len();
                                    for dst in dsts.iter() {
                                        let mut delta = ranks[src].clone() / dst_num as f32;
                                        session.give((dst.clone(), delta));

                                        println!("{} give {} the rank {} at {:?}", src , dst, delta, time);
                                    }
                                }

                                println!("Final PageRank of this iteration: {:?} \n", new_ranks);
                            }
                        }
                        rank_stash.retain(|_key, val| !val.is_empty());
                    }
                });
            
            output
            	.probe_with(&mut probe)
                .connect_loop(handle);

        });


        // feed the dataflow with data.
        // round 0: feed the data extracted from the hard disk file
        let mut round = 0;
        
        // send all edge with same timestamp
        for (src, dst) in edge_list {
        	edge_input.send((src, dst));
        }
        edge_input.advance_to(round+2);
        while probe.less_than(edge_input.time()) {
            worker.step();
        }
        //provide further edge input, if any.
    }).unwrap();
}

// use hash value for exchange partition
fn hash<T: Hash>(item: &T) -> u64 {
    let mut h = DefaultHasher::new();
    item.hash(&mut h);
    h.finish()
}

// read the edges from the hard disk file
fn get_edge_list_from_path(filename: String) -> (Vec<(String, String)> , i64){
	let f = File::open(filename).unwrap();
	let mut file = BufReader::new(&f);
	let mut edge_lsit: Vec<(String, String)> = Vec::new();
    let mut node_list = HashSet::new();
	for line in file.lines() {
        let l = line.unwrap();
        let mut tuple = l.split_whitespace();
        let mut index = 0;
        let mut src = "";
        let mut dst = "";
        for node in tuple{
        	if (index == 0) {
        		dst = node;
        	} else {
        		src = node;
        	}
        	index+=1;
        }
        node_list.insert(src.to_string());
        node_list.insert(dst.to_string());
        edge_lsit.push((src.to_string(), dst.to_string()));
    }

    let mut node_number = node_list.len();
    (edge_lsit, node_number as i64)
}