#ifndef LOGGER_HPP
#define LOGGER_HPP
#include <fstream>
#include <iostream>
#include <string>
#include <cassert>
#include "backing_store.hpp"

class Logger {
public:
    Logger(backing_store* storage, uint64_t persistence_granularity)
        : storage(storage), persistence_granularity(persistence_granularity), log_count(0) {
        log_stream.open("wal_log.txt", std::ios::out | std::ios::app);  // file handling
        if (!log_stream.is_open()) {
            std::cerr << "Failed to open log file for WAL" << std::endl;
            exit(1);
        }

    }

    ~Logger() {
        if (log_stream.is_open()) {
            log_stream.close();
        }
    }

    // Log for insert
    void log_insert(uint64_t key, const std::string& value) {
        std::cout << "Log Inserting..." << std::endl;
        log_entry("INSERT", key, value);
    }

    // Log for update
    void log_update(uint64_t key, const std::string& value) {
        std::cout << "Log Updating..." << std::endl;
        log_entry("UPDATE", key, value);
    }

    // Log for delete
    void log_delete(uint64_t key) {
        std::cout << "Log Deleting..." << std::endl;
        log_entry("DELETE", key, "");
    }

private:
    // Function to log any entry and handle persistence
    void log_entry(const std::string& operation, uint64_t key, const std::string& value) {
        if (!log_stream.is_open()) {
            std::cerr << "log_stream is not open!" << std::endl;
            return;  // Exit if log_stream isn't open to avoid segmentation faults
        }

        log_stream << operation << " " << key;
        if (!value.empty()) {
            log_stream << " " << value;
        }
        log_stream << std::endl;
        log_count++;

        // Persist if log count reaches persistence granularity
        if (log_count >= persistence_granularity) {
            persist();
        }
    }

    // Flush the log file to disk
    void persist() {
        std::cerr << "Persisting log to disk..." << std::endl;
        if (!log_stream.is_open() || log_stream.bad()) {
            std::cerr << "Cannot persist: log_stream is not in a good state" << std::endl;
            return;
        }
        log_stream.flush();
        log_count = 0;  // Reset count after persisting
        std::cerr << "Persisted successfully" << std::endl;
    }


    backing_store* storage;
    std::ofstream log_stream;
    uint64_t persistence_granularity;
    uint64_t log_count;
};

#endif // LOGGER_HPP
