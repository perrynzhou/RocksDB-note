#pragma once
#ifndef CHEAPIS_SERVER_H
#define CHEAPIS_SERVER_H

#include <atomic>
#include <string>
#include <thread>

#include "gujia.h"
#include "gujia_impl.h"
#include "resp_machine.h"
#include "rocksdb/env.h"

namespace rocksdb {
class DBImpl;
}

namespace cheapis {
using namespace gujia;

struct ServerRunner;

int ServerMain(ServerRunner* runner, rocksdb::DBImpl* db,
               const std::string& path, rocksdb::Env* env,
               rocksdb::Logger* log);

struct Client {
  RespMachine resp;
  std::string input;
  std::string output;
  long last_mod_time;
  unsigned int ref_count = 0;
  unsigned int consume_len = 0;
  bool close = false;

  explicit Client(long last_mod_time = -1) : last_mod_time(last_mod_time) {}
};

struct ServerRunner {
  ServerRunner(rocksdb::DBImpl* db, const std::string& path, rocksdb::Env* env,
               rocksdb::Logger* log) {
    auto p = &path;
    std::thread job([this, db, p, env, log]() {
      ServerMain(this, db, *p, env, log);
      closed_ = true;
    });
    job.detach();
  }

  ~ServerRunner() { assert(closing_ && closed_); }

  std::atomic<bool> closing_{false};
  std::atomic<bool> closed_{false};
};
}  // namespace cheapis

#endif  // CHEAPIS_SERVER_H