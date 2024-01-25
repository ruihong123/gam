#ifndef __DATABASE_UTILS_CLUSTER_CONFIG_H__
#define __DATABASE_UTILS_CLUSTER_CONFIG_H__

#include <string>
#include <fstream>
#include <vector>

namespace Database {
struct ServerInfo {
  ServerInfo(const std::string& addr, const int port)
      : addr_(addr),
        port_no_(port) {
  }
  ServerInfo() {
  }

  std::string addr_;
  int port_no_;
};

class ClusterConfig {
 public:
  ClusterConfig(const std::string& my_host_name, const int port, 
      const std::string& config_filename1, const std::string& config_filename2)
      : my_info_(my_host_name, port), compute_config_filename_(config_filename1),
      memory_config_filename_(config_filename2) {
    this->ReadConfigFile();
  }
  ~ClusterConfig() {
  }

  ServerInfo GetMyHostInfo() const {
    return my_info_;
  }

  ServerInfo GetMasterHostInfo() const {
    return computes_info_.at(0);
  }

  size_t GetPartitionNum() const {
    return computes_info_.size();
  }
    size_t GetMemoryNum() const {
        return memories_info_.size();
    }

  size_t GetMyPartitionId() const {
    for (size_t i = 0; i < computes_info_.size(); ++i) {
      ServerInfo host = computes_info_.at(i);
      if (host.addr_ == my_info_.addr_ ) { //&& host.port_no_ == my_info_.port_no_
        return i;
      }
    }
    assert(false);
    return computes_info_.size() + 1;
  }

  bool IsMaster() const {
    ServerInfo my = GetMyHostInfo();
    ServerInfo master = GetMasterHostInfo();
    return my.addr_ == master.addr_;// && my.port_no_ == master.port_no_;
  }

private:
  void ReadConfigFile() {
    std::string name;
    int port;

    std::ifstream computereadfile(compute_config_filename_);
    assert(computereadfile.is_open() == true);
    while (!computereadfile.eof()) {
      name = "";
      port = -1;
      computereadfile >> name >> port;
      if (name == "" && port < 0)
        continue;
      computes_info_.push_back(ServerInfo(name, port));
    }
    computereadfile.close();

    for (auto& entry : computes_info_) {
      std::cout << "server name=" << entry.addr_ << ",port_no="
                << entry.port_no_ << std::endl;
    }

      std::ifstream memoryreadfile(compute_config_filename_);
      assert(memoryreadfile.is_open() == true);
      while (!memoryreadfile.eof()) {
          name = "";
          port = -1;
          memoryreadfile >> name >> port;
          if (name == "" && port < 0)
              continue;
          memories_info_.push_back(ServerInfo(name, port));
      }
      memoryreadfile.close();

      for (auto& entry : memories_info_) {
          std::cout << "server name=" << entry.addr_ << ",port_no="
                    << entry.port_no_ << std::endl;
      }
  }
private:
  std::vector<ServerInfo> computes_info_;
  std::vector<ServerInfo> memories_info_;
  ServerInfo my_info_;
  std::string compute_config_filename_;
  std::string memory_config_filename_;
};
}

#endif
