#!/bin/bash
sleep 5
echo SETUP.sh time now: `date +"%T" `
mongo --host mongodb:27017 <<EOF
  var cfg = {
    "_id": "test-rs",
    "version": 1,
    "members": [
      {
        "_id": 0,
        "host": "localhost:27017"
      }
    ]
  };
  rs.initiate(cfg, { force: true });
  while (! db.isMaster().ismaster ) { sleep(1000) };
  db.getMongo().setReadPref('primary');
  rs.status();
EOF

