## Project
Given a desired departure, or arrival time, our route planner will compute the fastest route between two stops providing uncertainty tolerance expressed as probabilities.

Project done during the EE-490(h) class (Lab in Data Sciences) taught at EPFL.

### Demo

https://youtu.be/g5PTp1hs3wk

### Authors
- Diana Petrescu
- Isaac Leimgruber
- Patrik Wagner
- Thevie Mortiniera
- Kevin Kappel


## Problem Motivation
"Imagine you are a regular user of the public transport system, and you are checking the operator’s schedule to meet your friends for a class reunion. The choices are:

- You could leave in 10mins, and arrive with enough time to spare for gossips before the reunion starts.

- You could leave now on a different route and arrive just in time for the reunion.

Undoubtedly, if this is the only information available, most of us will opt for option 1.

If we now tell you that option 1 carries a fifty percent chance of missing a connection and be late for the reunion. Whereas, option 2 is almost guaranteed to take you there on time. Would you still consider option 1?

Probably not. However, most public transport applications will insist on the first option. This is because they are programmed to plan routes that offer the shortest travel times, without considering the risk factors."

## Files
The final notebook with CSA solution is final.ipynb and it can be runned it on the cluster.
Some helper functions helpers.py.
An other algorithm version is Cluster_idea.ipynb (not to be considered as it was too slow)


### Future improvements
1. Find categories for stop names and trip id delays in order to reduce the dimension and so the running time
2. Modify connections iterations to keep connections that we would miss but could use if the train has an usual slight delay
3. Fill missing values the departure / arrival time by performing profiling of trips / stops

### Reference
[CSA paper](https://i11www.iti.kit.edu/extra/publications/dpsw-isftr-13.pdf)
