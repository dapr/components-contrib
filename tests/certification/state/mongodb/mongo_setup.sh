#!/bin/bash
sleep 5
echo SETUP.sh time now: `date +"%T" `
mongosh --host mongo1:27017 <<EOF
  var cfg = {
    "_id": "rs0",
    "version": 1,
    "members": [
      {
        "_id": 0,
        "host": "mongo1:27017",
        "priority": 2
      },
      {
        "_id": 1,
        "host": "mongo2:27017",
        "priority": 0
      },
      {
        "_id": 2,
        "host": "mongo3:27017",
        "priority": 0
      }
    ]
  };
  rs.initiate(cfg, { force: true });
  while (! db.isMaster().ismaster ) { sleep(1000) };
  db.getMongo().setReadPref('primary');
  rs.status();
EOF

