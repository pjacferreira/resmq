# FEATURE SET for resmq Package

List of Feature implemented (or to be implemented) for the Package.

* ATOMIC Actions on a per queue basis (All System should be atomic in nature
  i.e. no other action can be performed, on the same queue, when another Actions
  is already being processed)

## QUEUE

* Manage any number of queues
  * ADD queue
  * DELETE queue
  * LIST queues
  * LIST queue messages
  * Retrieve a Message from the queue, in any state (ACTIVE, IN PROCESSING, ANY)
* Allow for Message Lifetimes, on per queue basis
* Allow for Message Processing Limit, on a per queue basis

## MESSAGES

* Allow for any type of message (i.e. all messages are treated as strings, no
  specific type used)
* Allow for moving messages between queues (maintain same ID and properties)
