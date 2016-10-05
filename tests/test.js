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
    system.queueExists("test");
    system.queue("test");
  })
  .on("disconnect", function() {
    console.log("REDIS: Diconnect");
  })
  .on("queues", function(resp) {
    console.log("Queues List [" + (resp ? resp.join(" , ") : "") + "]");
  })
  .on("queue-exists", function(name, exists) {
    console.log("Queue [" + name + "] " + (exists ? "exists" : "does not exist"));
  })
  .on("queue", function(q) {
    console.log("Loaded Queue [" + q.name + "]");
    system.queueExists("test");
    q
      .post(null, "TEST MESSAGE")
      .on("error", function(err) {
        console.log(err.message);
        system.quit();
      })
      .on("new-message", function(msg) {
        console.log("New Message [" + msg._id + "] - [" + msg._message + "]");
        q.peek();
      })
      .on("peek-message", function(msg) {
        console.log("Peek Message [" + msg._id + "] - [" + msg._message + "]");
        q.pop();
      })
      .on("message", function(msg) {
        console.log("Receive Message [" + msg._id + "] - [" + msg._message + "]");
      })
      .on("removed-message", function(msg) {
        console.log("Delete Message [" + msg._id + "] - [" + msg._message + "]");
        system.quit();
      });
  })
  .on("error", function(err) {
    console.log(err.message);
    system.quit();
  });
