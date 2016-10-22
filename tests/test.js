var _ = require("lodash");
var Queues = require("../index.js");

var system = new Queues({
  host: "10.137.6.35",
  options: {
    password: "rvKTk6xH8bDapzp6G5F9",
    db: 1
  }
});

system
  .on("connect", function() {
    console.log("REDIS: Connected");

    // List Queues
    system.queues();
    system.existsQueue("test");
    system.queue("test");
  })
  .on("disconnect", function() {
    console.log("REDIS: Diconnect");
  })
  .on("queues", function(list) {
    console.log("Queues List [" + (list ? list.join(" , ") : "") + "]");
  })
  .on("queue-exists", function(exists, name) {
    console.log("Queue [" + name + "] " + (exists ? "exists" : "does not exist"));
  })
  .on("queue", function(q) {
    console.log("Loaded Queue [" + q.name + "]");

    // RE-TEST for Queue Existance and Post Message using System
    system
      .on("message-new", function(msg) {
        console.log("New Message [" + msg.id() + ":" + msg._props.created + "] in Queue [" + msg.queue() + "] - [" + msg.message() + "]");
      })
      .existsQueue("test")
      .postJSON({ message: "TEST MESSAGE 1"}, "test");

    // USE Queue to Post Message
    q
      .postJSON({ message: "TEST MESSAGE 2"})
      .on("error", function(err) {
        console.log(err.message);
        system.quit();
      })
      .on("message-new", function(msg) {
        console.log("New Message [" + msg.id() + ":" + msg._props.created + "] - [" + msg.message() + "]");
        q.peek();

        // q.pending(); // PROBLEM 001 : RACE CONDITION
      })
      .on("message-peek", function(msg) {
        if (msg != null) {
          console.log("Peek Message [" + msg.id() + "] - [" + msg.message() + "]");
        } else {
          console.log("Peek Message: No Pending Messages");
        }

        // q.pop(); // PROBLEM 001 : RACE CONDITION
        q.pending();
      })
      .on("message", function(msg) {
        console.log("Receive [" + msg.id() + "] Received - [" + msg.message() + "]");
        msg
          .on("deleted", function(msg) {
            console.log("Message Deleted [" + msg.id() + "]");
            q.pending();
          })
          .delete();
      })
      .on("message-pop", function(msg) {
        console.log("Message [" + msg.id() + "] Received and Removed - [" + msg.message() + "]");
        q.pending();
      })
      .on("messages-pending", function(list) {
        console.log("Pending Messages [" + (list ? list.join(" , ") : "") + "]");
        if (list.length === 0) {
          system.quit();
        } else {
          if (list.length > 1) {
            q.pop();
          } else {
            q.receive();
          }
        }
      });
  })
  .on("error", function(err) {
    console.log(_.isString(err) ? err : err.message);
    system.quit();
  });

/* PROBLEM-001:
 * THERE IS A RACE CONDITION (i.e. the REDIS QUEUE is not completely atomic
 * in the commented code)
 * i.e. the run list 2 messages (sometimes correctly both ID and Message TEXT)
 * sometimes incorrectly SECOND entry has a NULL for the Message Text
 */
