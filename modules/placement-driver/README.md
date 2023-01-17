# Placement driver

The module responses for location of leaseholder node in the replication group. The placement driver decides a node which can hold a lease 
and approves with the replication group.

## Leaseholder management
Placement driver manager depends on Metastorage and a logical topology. The manager listens change of group members (due to a particular 
data zone shrink or growing) and appear / disappear nodes in logical topology. Every time when a new node become available (appeared in the 
topology and added to the group members) or unavailable (disappeared from the topology or removed to the group members) Placement driver 
choosing a leaseholder taking into these modifications.

The leaseholder will be able to be elected only when group members are enough. By the reason the module waits the members at first and then
the defines a leaseholder of the group.
